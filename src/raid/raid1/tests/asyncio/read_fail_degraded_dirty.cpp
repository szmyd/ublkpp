// Regression: degraded array + dirty bitmap region + active device async read failure must return
// -EAGAIN (transient, kernel can retry), not fall through to the backup which holds stale data.

#include "async_raid1_common.hpp"

using ::testing::NiceMock;

TEST(Raid1Async, ReadDegradedDirtyActiveFailNoFallback) {
    TestParams const pa{.capacity = AsyncRaid1Fixture::k_disk_cap, .id = "DiskA", .is_slot_b = false};
    TestParams const pb{.capacity = AsyncRaid1Fixture::k_disk_cap, .id = "DiskB", .is_slot_b = true};
    auto disk_a = std::make_shared< NiceMock< ublkpp::AsyncTestDisk > >(pa);
    auto disk_b = std::make_shared< NiceMock< ublkpp::AsyncTestDisk > >(pb);

    // Degraded superblock on disk_a: route=DEVA, unclean → __init_bitmap_and_degraded_route marks
    // bitmap fully dirty. Backup is reachable but has stale data.
    static const ublkpp::raid1::SuperBlock degraded_sb = {
        .header = {.magic = {0x53, 0x25, 0xff, 0x0a, 0x34, 0x99, 0x3e, 0xc5, 0x67, 0x3a, 0xc8, 0x17, 0x49, 0xae, 0x1b,
                             0x64},
                   .version = htobe16(1),
                   .uuid = {0xad, 0xa4, 0x07, 0x37, 0x30, 0xe3, 0x49, 0xfe, 0x99, 0x42, 0x5a, 0x28, 0x7d, 0x71, 0xeb,
                            0x3f}},
        .fields = {.clean_unmount = 0,
                   .read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA),
                   .device_b = 0,
                   .bitmap = {._reserved = {0x00}, .chunk_size = htobe32(32 * Ki), .age = htobe64(2)}},
        .superbitmap_reserved = {0x00}};

    ON_CALL(*disk_a, prepare(_, _)).WillByDefault(Return(std::vector< int >{}));
    ON_CALL(*disk_b, prepare(_, _)).WillByDefault(Return(std::vector< int >{}));

    ON_CALL(*disk_a, sync_iov(_, _, _, _)).WillByDefault([](uint8_t op, iovec* iovecs, uint32_t, off_t) -> io_result {
        if (op == UBLK_IO_OP_READ && iovecs && iovecs->iov_base)
            memcpy(iovecs->iov_base, &degraded_sb, ublkpp::raid1::k_page_size);
        return static_cast< int >(iovecs->iov_len);
    });
    ON_CALL(*disk_b, sync_iov(_, _, _, _)).WillByDefault([](uint8_t op, iovec* iovecs, uint32_t, off_t) -> io_result {
        if (op == UBLK_IO_OP_READ && iovecs && iovecs->iov_base) {
            memcpy(iovecs->iov_base, &async_raid1_superblock, ublkpp::raid1::k_page_size);
            static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;
            static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.bitmap.age = htobe64(1);
        }
        return static_cast< int >(iovecs->iov_len);
    });

    ON_CALL(*disk_a, submit_iov(_, _, _, _, _)).WillByDefault(make_async_iov_action());
    ON_CALL(*disk_b, submit_iov(_, _, _, _, _)).WillByDefault(make_async_iov_action());

    auto raid = std::make_shared< ublkpp::raid1::Raid1Disk >(
        boost::uuids::string_generator()(std::string(AsyncRaid1Fixture::k_uuid)), disk_a, disk_b);
    auto mock = std::make_unique< ublkpp::MockUblksrv >(raid);

    // Confirm degraded with fully dirty bitmap; backup not marked unavail.
    ASSERT_GT(raid->replica_states().bytes_to_sync, 0u);
    raid->toggle_resync(false);

    // Active device is called once; backup must not be touched (it has stale data).
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 1u);

    // Active fails → !failover_dev (dirty) → -EAGAIN immediately.
    auto completions = mock->inject_cqe(0, -EIO);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EAGAIN);
}
