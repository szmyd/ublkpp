// Regression for H8: when both devices have equal superblock ages but only one had a clean
// unmount, the array must open in degraded mode routing reads to the clean device.
//
// Previously pick_superblock returned the clean device's SB with read_route=EITHER, which
// caused __init_bitmap_and_degraded_route to treat the array as fully healthy even though the
// unclean replica may hold partially-written data.

#include "test_raid1_common.hpp"

using ::testing::_;
using ::testing::NiceMock;

static ublkpp::raid1::SuperBlock make_sb(uint64_t age, uint8_t clean_unmount, uint8_t device_b_flag) {
    auto sb = normal_superblock;
    sb.fields.bitmap.age = htobe64(age);
    sb.fields.clean_unmount = clean_unmount;
    sb.fields.device_b = device_b_flag;
    return sb;
}

// H8a: Device A crashed (unclean), Device B shut down cleanly — same age.
// Expected: array opens degraded routing reads to B (device_a == SYNCING).
TEST(Raid1, OpenAsymmetricUncleanADegraded) {
    auto raw_a = std::make_shared< NiceMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< NiceMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    auto const sb_a = make_sb(2, /*clean=*/0, /*device_b=*/0);
    auto const sb_b = make_sb(2, /*clean=*/1, /*device_b=*/1);

    ON_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillByDefault([sb_a](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &sb_a, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    ON_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillByDefault([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    ON_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillByDefault([sb_b](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &sb_b, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    ON_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillByDefault([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);
    raid_device.toggle_resync(false);

    auto const states = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::SYNCING, states.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, states.device_b);
}

// H8b: Device A shut down cleanly, Device B crashed — same age.
// Expected: array opens degraded routing reads to A (device_b == SYNCING).
TEST(Raid1, OpenAsymmetricUncleanBDegraded) {
    auto raw_a = std::make_shared< NiceMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< NiceMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    auto const sb_a = make_sb(5, /*clean=*/1, /*device_b=*/0);
    auto const sb_b = make_sb(5, /*clean=*/0, /*device_b=*/1);

    ON_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillByDefault([sb_a](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &sb_a, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    ON_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillByDefault([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    ON_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillByDefault([sb_b](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &sb_b, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    ON_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillByDefault([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);
    raid_device.toggle_resync(false);

    auto const states = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, states.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::SYNCING, states.device_b);
}
