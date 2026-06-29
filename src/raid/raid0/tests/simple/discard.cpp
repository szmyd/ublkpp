#include "test_raid0_common.hpp"

// Verifies that Raid0Disk::prepare() sizes the CQE pool for the DISCARD fan-out of N disks,
// not the read/write fan-out k = stripes_for_io(max_tx, stripe_size, N).
//
// With stripe_size=128 KiB and max_tx=512 KiB, k = min((512/128)+1, N) = min(5, N).
// For N=10: k=5 < N=10. Before the fix, prepare() accumulated only 5 children's
// max_sqes_per_io; a full-stride DISCARD consuming all 10 pool slots hit RELEASE_ASSERT.
TEST(Raid0PreparePoolSize, DiscardFanoutRequiresAllChildren) {
    constexpr uint32_t kN = 10;
    std::vector< std::shared_ptr< ublk_disk > > disks;
    for (uint32_t i = 0; i < kN; ++i) {
        auto d = CREATE_DISK(TestParams{.capacity = Gi});
        EXPECT_CALL(*d, prepare(_, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly(Return(ublk_disk::prepare_result{.max_sqes_per_io = 1}));
        disks.push_back(std::move(d));
    }
    auto raid = ublkpp::make_raid0_disk(boost::uuids::string_generator{}(test_uuid), 128 * Ki, std::move(disks));
    auto result = raid->prepare(nullptr, 0);
    EXPECT_EQ(result.max_sqes_per_io, kN);
}
