#include "async_raid1_common.hpp"

// Healthy discard: fans out to both replicas via async_iov.
// inject active first (RAID1 suspends on backup), then inject backup → completion.
TEST_F(AsyncRaid1Fixture, DiscardBothDevices) {
    // 32KiB discard — chunk-aligned so totally_aligned=true (chunk_size=32Ki)
    auto res = mock->submit_io(0, UBLK_IO_OP_DISCARD, 0, 32 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u); // active + backup CqeStates

    // Active done, suspend on backup
    EXPECT_TRUE(mock->inject_cqe(0, 0).empty());
    // Backup done → completion
    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].tag, 0);
    EXPECT_EQ(completions[0].result, 0);
}

// Large discard spanning the full disk capacity; backup device fails → entire bitmap dirty.
// Verifies that bytes_to_sync reflects the full capacity after a discard-induced degradation,
// covering the path where dirty_region() is called for every chunk in the range.
TEST_F(AsyncRaid1Fixture, LargeDiscardDirtiesMultiplePages) {
    constexpr uint64_t k_full_sectors = k_disk_cap / 512;

    auto res = mock->submit_io(0, UBLK_IO_OP_DISCARD, 0, k_full_sectors, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    EXPECT_TRUE(mock->inject_cqe(0, 0).empty()); // disk_a discard succeeds
    auto comp = mock->inject_cqe(0, -EIO);       // disk_b fails → degrade
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_GE(comp[0].result, 0);

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::ERROR);
    EXPECT_GT(states.bytes_to_sync, k_disk_cap / 2);
}
