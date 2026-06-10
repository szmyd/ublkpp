// SyncIoDegradedSbPersistRetryFails
// Phase 1: active (raw_a) write succeeds, backup (raw_b) write fails; __become_degraded SB write
// to raw_a also fails → resource_unavailable_try_again. _degraded_sb_pending set.
// Phase 2: array in DEVA mode (raw_b ERROR); sync write routes to raw_a only (Site 2 — backup
// unavailable). __become_degraded delegates to __try_persist_degraded_sb; SB retry also fails →
// resource_unavailable_try_again. _degraded_sb_pending remains set until destructor.
//
// SyncIoDegradedSbPersistRetrySucceeds
// Phase 1: same as above.
// Phase 2: Site 2 fires; __try_persist_degraded_sb retry succeeds → write returns success.
//
// SyncIoBecomeDegradedSbFailRetrySucceeds
// Mirror of SyncIoDegradedSbPersistRetrySucceeds but with active (raw_a) failing (Site 1).
// Phase 1: raw_a write fails → __become_degraded SB write to raw_b fails → EAGAIN (no backup
// data write). Phase 2: DEVB mode (raw_b sole device) → Site 2 retries SB successfully → success.

#include "test_raid1_common.hpp"

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::StrictMock;

TEST(Raid1, SyncIoDegradedSbPersistRetryFails) {
    auto raw_a = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    // Normal superblock reads for init.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) {
                memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                static_cast< ublkpp::raid1::SuperBlock* >(iov->iov_base)->fields.device_b = 1;
            }
            return ublkpp::raid1::k_page_size;
        });
    // Pre-construction write catch-alls: absorb the two SB init writes the constructor makes.
    // Post-construction expectations (registered after the constructor) override via GMock LIFO.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);
    raid_device.toggle_resync(false);

    // Post-construction: specific expectations take LIFO priority over the catch-alls above.
    // Bitmap and data writes to raw_a at non-SB offsets always succeed.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Ne((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    // Phase 1: backup write fails (triggers __become_degraded; raw_b is marked ERROR afterwards).
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
    // SB writes to raw_a at offset 0: Phase-1 fails, Phase-2 retry fails, destructor succeeds.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .Times(3)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto const test_sz = static_cast< size_t >(4 * Ki);

    // Phase 1: active succeeds, backup fails → __become_degraded (Site 3) → SB write fails.
    {
        iovec iov{nullptr, test_sz};
        auto const res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 8 * Ki);
        ASSERT_FALSE(res);
        EXPECT_EQ(std::errc::resource_unavailable_try_again, res.error());
    }

    // Phase 2: DEVA mode, raw_b unavailable → Site 2 fires → __try_persist_degraded_sb fails.
    {
        iovec iov{nullptr, test_sz};
        auto const res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 16 * Ki);
        ASSERT_FALSE(res);
        EXPECT_EQ(std::errc::resource_unavailable_try_again, res.error());
    }
}

TEST(Raid1, SyncIoDegradedSbPersistRetrySucceeds) {
    auto raw_a = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) {
                memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                static_cast< ublkpp::raid1::SuperBlock* >(iov->iov_base)->fields.device_b = 1;
            }
            return ublkpp::raid1::k_page_size;
        });
    // Pre-construction write catch-alls: absorb the two SB init writes the constructor makes.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);
    raid_device.toggle_resync(false);

    // Post-construction: specific expectations take LIFO priority over the catch-alls above.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Ne((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    // Phase 1: backup write fails (triggers __become_degraded; raw_b is marked ERROR afterwards).
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
    // SB writes to raw_a at offset 0: Phase-1 fails, Phase-2 retry succeeds, destructor succeeds.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .Times(3)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto const test_sz = static_cast< size_t >(4 * Ki);

    // Phase 1: active succeeds, backup fails → __become_degraded (Site 3) → SB write fails.
    {
        iovec iov{nullptr, test_sz};
        auto const res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 8 * Ki);
        ASSERT_FALSE(res);
        EXPECT_EQ(std::errc::resource_unavailable_try_again, res.error());
    }

    // Phase 2: DEVA mode, raw_b unavailable → Site 2 fires → __try_persist_degraded_sb succeeds.
    {
        iovec iov{nullptr, test_sz};
        auto const res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 16 * Ki);
        ASSERT_TRUE(res);
        EXPECT_EQ(test_sz, *res);
    }
}

TEST(Raid1, SyncIoBecomeDegradedSbFailRetrySucceeds) {
    auto raw_a = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) {
                memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                static_cast< ublkpp::raid1::SuperBlock* >(iov->iov_base)->fields.device_b = 1;
            }
            return ublkpp::raid1::k_page_size;
        });
    // Pre-construction write catch-alls: absorb the two SB init writes the constructor makes.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);
    raid_device.toggle_resync(false);

    // Post-construction: specific expectations take LIFO priority over the catch-alls above.
    // Phase 1: active (raw_a) data write fails; no backup data write occurs (Site 1 returns EAGAIN
    // when __become_degraded fails before the backup write). Destructor writes nothing to raw_a.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Ne((off_t)0)))
        .Times(1)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
    // Catch-all for raw_b data writes (addr != 0): Phase-2 data write and destructor bitmap.
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Ne((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    // SB writes to raw_b at offset 0: Phase-1 fails, Phase-2 retry succeeds, destructor succeeds.
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .Times(3)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto const test_sz = static_cast< size_t >(4 * Ki);

    // Phase 1: active fails → __become_degraded (Site 1) → SB write to raw_b fails → EAGAIN.
    // No backup data write: Site 1 returns EAGAIN immediately when __become_degraded fails.
    {
        iovec iov{nullptr, test_sz};
        auto const res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 8 * Ki);
        ASSERT_FALSE(res);
        EXPECT_EQ(std::errc::resource_unavailable_try_again, res.error());
    }
    EXPECT_EQ(raid_device.replica_states().device_a, ublkpp::raid1::replica_state::ERROR);
    EXPECT_EQ(raid_device.replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);

    // Phase 2: DEVB mode, raw_a unavailable → Site 2 fires → __try_persist_degraded_sb succeeds.
    {
        iovec iov{nullptr, test_sz};
        auto const res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 16 * Ki);
        ASSERT_TRUE(res);
        EXPECT_EQ(test_sz, *res);
    }
}
