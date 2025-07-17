#include "test_raid1_common.hpp"

TEST(Raid1, PickSuper) {
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 0}},
                                      ._reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 0}},
                                      ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice.first, &deva_sb);
        EXPECT_EQ(choice.second, ublkpp::raid1::read_route::EITHER);
    }
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 0}},
                                      ._reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 1}},
                                      ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice.first, &devb_sb);
        EXPECT_EQ(choice.second, ublkpp::raid1::read_route::DEVB);
    }
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 1}},
                                      ._reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 1, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 1}},
                                      ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice.first, &devb_sb);
        EXPECT_EQ(choice.second, ublkpp::raid1::read_route::EITHER);
    }
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 2}},
                                      ._reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 1, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 1}},
                                      ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice.first, &deva_sb);
        EXPECT_EQ(choice.second, ublkpp::raid1::read_route::DEVA);
    }
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 1, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 2}},
                                      ._reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0, .bitmap = {.chunk_size = 0, .dirty = 0, .age = 2}},
                                      ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice.first, &deva_sb);
        EXPECT_EQ(choice.second, ublkpp::raid1::read_route::EITHER);
    }
}

TEST(Raid1, CalcBitmapRegions) {
    static uint32_t chunk_size = 32 * Ki;
    static uint32_t page_width = chunk_size * ublkpp::raid1::k_page_size *
        ublkpp::raid1::k_bits_in_byte;                              // How many user data bytes does a BITMAP page cover
    static uint32_t word_width = chunk_size * sizeof(uint64_t) * 8; // How many user data bytes does a BITMAP WORD cover

    using ublkpp::raid1::calc_bitmap_region;
    // Test simple first chunk dirty
    {
        auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(0, 4 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(63, shift_offset);
        EXPECT_EQ(4 * Ki, sz);
    }

    // Still in first chunk
    {
        auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(4 * Ki, chunk_size, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(63, shift_offset);
        EXPECT_EQ(chunk_size, sz);
    }

    // Second chunk (still in first word and first page)
    {
        auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(chunk_size, 16 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(62, shift_offset);
        EXPECT_EQ(16 * Ki, sz);
    }

    // Last bit (chunk) of the first word and page
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            calc_bitmap_region((chunk_size * 64) - 4 * Ki, 16 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(0, shift_offset);
        EXPECT_EQ(16 * Ki, sz);
    }

    // Middle bit (chunk) of the first word and page of differing chunk size
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            calc_bitmap_region((chunk_size * 64) - 4 * Ki, 16 * Ki, chunk_size * 2);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(32, shift_offset);
        EXPECT_EQ(16 * Ki, sz);
    }

    // Second word; first Chunk
    {
        auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(word_width, 16 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(1, word_offset);
        EXPECT_EQ(63, shift_offset);
        EXPECT_EQ(16 * Ki, sz);
    }

    // Second page, all in the first word
    {
        auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(page_width, 128 * Ki, chunk_size);
        EXPECT_EQ(1, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(63, shift_offset);
        EXPECT_EQ(128 * Ki, sz);
    }

    // First page last word and bit, sz is truncated at page boundary
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            calc_bitmap_region(page_width - (chunk_size), 128 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(511, word_offset);
        EXPECT_EQ(0, shift_offset);
        EXPECT_EQ(chunk_size, sz);
    }

    // First page, last word and bit offset into chunk, truncated at page boundary
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            calc_bitmap_region(page_width - (4 * Ki), 12 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(511, word_offset);
        EXPECT_EQ(0, shift_offset);
        EXPECT_EQ(4 * Ki, sz);
    }

    // First page, last word and bit offset into chunk, truncated at page boundary
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            calc_bitmap_region(page_width - (4 * Ki), 2 * chunk_size, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(511, word_offset);
        EXPECT_EQ(0, shift_offset);
        EXPECT_EQ(4 * Ki, sz);
        {
            auto [pg_offset2, word_offset2, shift_offset2, sz2] =
                calc_bitmap_region(page_width - (4 * Ki) + (sz), (2 * chunk_size) - sz, chunk_size);
            EXPECT_EQ(1, pg_offset2);
            EXPECT_EQ(0, word_offset2);
            EXPECT_EQ(63, shift_offset2);
            EXPECT_EQ((2 * chunk_size) - sz, sz2);
        }
    }

    // Third page, middle of second word
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            calc_bitmap_region((page_width * 2) + word_width + (3 * chunk_size), 5 * chunk_size, chunk_size);
        EXPECT_EQ(2, page_offset);
        EXPECT_EQ(1, word_offset);
        EXPECT_EQ(60, shift_offset);
        EXPECT_EQ(5 * chunk_size, sz);
    }
}

// Brief: If either devices should not load/write superblocks correctly, initialization should throw
TEST(Raid1, FailedReadSBDevA) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, false, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, false, false);
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}
TEST(Raid1, FailedReadSBDevB) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, true, false);
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}
TEST(Raid1, FailedReadSBDevBoth) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, true, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, true, false);
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}

// Should not throw just dirty SB
TEST(Raid1, FailedUpdateSBDevA) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_b);
}

// Should not throw just dirty SB
TEST(Raid1, FailedUpdateSBDevB) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    // Expect an extra WRITE to the SB when sync'ing the SB to DevB fails
    EXPECT_SYNC_OP_REPEAT(UBLK_IO_OP_WRITE, 2, device_a, false, ublkpp::raid1::k_page_size, 0UL);
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
}

TEST(Raid1, FailedUpdateSBDevBoth) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}

TEST(Raid1, FailedSecondUpdateDevA) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    // Expect an extra WRITE to the SB when sync'ing the SB to DevB fails
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iov, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}

// Brief: Test that RAID1 devices correctly report paramaters based on Devices A and B
//
// Construct a RAID1 device with Identical underlying devices that match on every
// parameter. The final RAID1 parameters should be equivalent to the underlying
// devices themselves.
TEST(Raid1, IdenticalDeviceProbing) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    EXPECT_EQ(raid_device.capacity(), (Gi)-reserved_size);
    EXPECT_STREQ(raid_device.type().c_str(), "Raid1");

    // CanDiscard and DirectIO `true` be default.
    EXPECT_EQ(raid_device.can_discard(), true);
    EXPECT_EQ(raid_device.direct_io, true);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test that RAID1 devices correctly report paramaters based on Devices A and B
//
// Construct a RAID1 device with Differing underlying devices that deviate on every
// parameter. The final RAID1 parameters should represent the lowest feature set of
// both devices including Capacity, BlockSize, Discard
TEST(Raid1, DiffereingDeviceProbing) {
    auto device_a = CREATE_DISK((TestParams{.capacity = 5 * Gi, .l_size = 512, .p_size = 8 * Ki}));
    auto device_b =
        CREATE_DISK((TestParams{.capacity = 3 * Gi, .l_size = 4 * Ki, .p_size = 4 * Ki, .can_discard = false}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    // Smallest disk was 3GiB
    EXPECT_EQ(raid_device.capacity(), (3 * Gi) - reserved_size);

    // LBS/PBS represent by shift size, not raw byte count
    EXPECT_EQ(raid_device.block_size(), 4 * Ki);
    EXPECT_EQ(raid_device.params()->basic.physical_bs_shift, ilog2(8 * Ki));

    // Device B lacks Discard support
    EXPECT_EQ(raid_device.can_discard(), false);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test that RAID1 array maintains a self-imposing limit to restrict the reserved size
//
TEST(Raid1, DevicesLargerThanAllowed) {
    auto device_a = CREATE_DISK(TestParams{.capacity = ublkpp::raid1::k_max_dev_size + ublkpp::Ti});
    auto device_b = CREATE_DISK(TestParams{.capacity = ublkpp::raid1::k_max_dev_size * 2});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    EXPECT_EQ(raid_device.capacity(), ublkpp::raid1::k_max_dev_size);
    EXPECT_STREQ(raid_device.type().c_str(), "Raid1");

    // CanDiscard and DirectIO `true` be default.
    EXPECT_EQ(raid_device.can_discard(), true);
    EXPECT_EQ(raid_device.direct_io, true);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test that we open the underlying devices correctly, and return them to our upper layer.
//
// When a UblkDisk receives a call to `open_for_uring`, it's expected to return a std::set of all
// fds that were opened by the underlying Devices in order to register them with io_uring. Test
// that RAID1 is collecting these FDs and passing the io_uring offset to the lower layers.
TEST(Raid1, OpenDevices) {
    static const auto start_idx = 2;
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    // Each device should be subsequently opened and return a set with their sole FD.
    EXPECT_CALL(*device_a, open_for_uring(_)).Times(1).WillOnce([](int const fd_off) {
        EXPECT_EQ(start_idx, fd_off);
        // Return 2 FDs here, maybe it's another RAID1 device?
        return std::list< int >{INT_MAX - 2, INT_MAX - 3};
    });
    EXPECT_CALL(*device_b, open_for_uring(_)).Times(1).WillOnce([](int const fd_off) {
        // Device A took 2 uring offsets
        EXPECT_EQ(start_idx + 2, fd_off);
        return std::list< int >{INT_MAX - 1};
    });

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    auto fd_list = raid_device.open_for_uring(2);
    EXPECT_EQ(3, fd_list.size());
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 3)));
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 2)));
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 1)));

    EXPECT_CALL(*device_a, collect_async(_, _)).Times(0);
    EXPECT_CALL(*device_b, collect_async(_, _)).Times(0);
    std::list< ublkpp::async_result > result_list;
    raid_device.collect_async(nullptr, result_list);
    ASSERT_EQ(0, result_list.size());

    device_a->uses_ublk_iouring = false;
    device_b->uses_ublk_iouring = false;

    EXPECT_CALL(*device_a, collect_async(_, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, std::list< ublkpp::async_result >& compls) {
            compls.push_back(ublkpp::async_result{nullptr, 0, 5});
        });
    EXPECT_CALL(*device_b, collect_async(_, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, std::list< ublkpp::async_result >& compls) {
            compls.push_back(ublkpp::async_result{nullptr, 1, 10});
        });

    raid_device.collect_async(nullptr, result_list);

    ASSERT_EQ(2, result_list.size());
    EXPECT_EQ(0, result_list.begin()->sub_cmd);
    EXPECT_EQ(5, result_list.begin()->result);
    EXPECT_EQ(1, (++result_list.begin())->sub_cmd);
    EXPECT_EQ(10, (++result_list.begin())->result);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

TEST(Raid1, UnknownOp) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto ublk_data = make_io_data(0xFF, 4 * Ki, 8 * Ki);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
    remove_io_data(ublk_data);
    ASSERT_FALSE(res);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test a READ through the RAID1 Device. We should only receive the READ on one of the
// two underlying replicas.
TEST(Raid1, SimpleRead) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                         uint32_t nr_vecs, uint64_t addr) {
                // The route should shift up by 1
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_EQ(nr_vecs, 1);
                // It should not have the REPLICATE bit set
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_FALSE(ublkpp::is_retry(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_READ, 4 * Ki, 8 * Ki);
        auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }
    // Reads-Round-Robin
    {
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                         uint32_t nr_vecs, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_EQ(nr_vecs, 1);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_FALSE(ublkpp::is_retry(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_READ, 4 * Ki, 8 * Ki);
        auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test retrying a READ within the RAID1 Device, if the CLEAN device fails immediately
TEST(Raid1, FailoverRead) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            // It should also have the RETRIED bit set
            EXPECT_FALSE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            // It should also have the RETRIED bit set
            EXPECT_TRUE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });

    auto ublk_data = make_io_data(UBLK_IO_OP_READ, 4 * Ki, 12 * Ki);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test retrying a READ through the RAID1 Device, and subsequent READs now go to B
//
// A failed read does not prevent us from continuing to try and read from the device, it must
// experience a failure to mutate, so this immediate read failure still has the follow-up read
// attempt on device A.
TEST(Raid1, ReadRetryA) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b11);
            // It should also have the RETRIED bit set
            EXPECT_TRUE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });

    auto ublk_data = make_io_data(UBLK_IO_OP_READ, 4 * Ki, 12 * Ki);
    // Construct a Retry Route that points to Device A in a RAID1 device
    auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b10}, ublkpp::sub_cmd_flags::RETRIED);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, sub_cmd);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // Now test the normal path
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            // The route has changed to point to device_b
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            // It should not have the RETRIED bit set
            EXPECT_FALSE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });

    ublk_data = make_io_data(UBLK_IO_OP_READ);
    // Construct a Non-Retry Route
    sub_cmd = ublkpp::sub_cmd_t{0b10};
    res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 12 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Identical to ReadRetryA but for Device B.
TEST(Raid1, ReadRetryB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            // It should also have the RETRIED bit set
            EXPECT_TRUE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 64 * Ki);
            EXPECT_EQ(addr, (32 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

    auto ublk_data = make_io_data(UBLK_IO_OP_READ);
    auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101}, ublkpp::sub_cmd_flags::RETRIED);
    auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 64 * Ki, 32 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test basic __failover_read functionality
TEST(Raid1, SyncIoReadDevAFail) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_READ;
    auto const test_off = 8 * Ki;
    auto const test_sz = 80 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + ublkpp::raid1::reserved_size);
    EXPECT_SYNC_OP(test_op, device_b, false, test_sz, test_off + ublkpp::raid1::reserved_size);

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, res.value());

    // expect unmount_clean on both (READ fails do not dirty bitmap)
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// This test is similar to SyncIoDevAFail, but the re-issued READ fails too. Still device is *NOT* degraded!
TEST(Raid1, SyncIoReadFailBoth) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_READ;
    auto const test_off = 64 * Ki;
    auto const test_sz = 1024 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + ublkpp::raid1::reserved_size);
    EXPECT_SYNC_OP(test_op, device_b, true, test_sz, test_off + ublkpp::raid1::reserved_size);

    ASSERT_FALSE(raid_device.sync_io(test_op, nullptr, test_sz, test_off));

    // expect attempt to sync both SBs
    EXPECT_TO_WRITE_SB_F(device_a, true);
    EXPECT_TO_WRITE_SB_F(device_b, true);
}

// Brief: Degrade the array and then fail read; it should not attempt failover read from dirty regions
TEST(Raid1, ReadOnDegraded) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    // First send a retry write to degrade the array on side A
    {
        // Will dirty superblock header
        EXPECT_TO_WRITE_SB(device_b);
        // Will dirty superblock page
        EXPECT_TO_WRITE_SB_ASYNC(device_b);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }

    // Now send retry reads for the region; they should fail immediately
    {
        auto ublk_data = make_io_data(UBLK_IO_OP_READ);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 64 * Ki, 4 * Ki);
        remove_io_data(ublk_data);
        EXPECT_FALSE(res);
    }
    // Retries from non-dirty chunks go through
    {
        EXPECT_CALL(*device_a, async_iov(UBLK_IO_OP_READ, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                // It should also have the RETRIED bit set
                EXPECT_TRUE(ublkpp::is_retry(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 64 * Ki);
                EXPECT_EQ(addr, (128 * Ki) + reserved_size);
                return 1;
            });
        auto ublk_data = make_io_data(UBLK_IO_OP_READ);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 64 * Ki, 128 * Ki);
        remove_io_data(ublk_data);
        EXPECT_TRUE(res);
    }
    // Retries from the degraded device are fine
    {
        EXPECT_CALL(*device_b, async_iov(UBLK_IO_OP_READ, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                // It should also have the RETRIED bit set
                EXPECT_TRUE(ublkpp::is_retry(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 64 * Ki);
                EXPECT_EQ(addr, (4 * Ki) + reserved_size);
                return 1;
            });
        auto ublk_data = make_io_data(UBLK_IO_OP_READ);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 64 * Ki, 4 * Ki);
        remove_io_data(ublk_data);
        EXPECT_TRUE(res);
    }

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_b);
}

// Flush is a no-op in RAID1
TEST(Raid1, FlushRetry) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, handle_flush(_, _, _)).Times(0);
    EXPECT_CALL(*device_b, handle_flush(_, _, _)).Times(0);
    auto ublk_data = make_io_data(UBLK_IO_OP_FLUSH);
    auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, sub_cmd);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(0, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
