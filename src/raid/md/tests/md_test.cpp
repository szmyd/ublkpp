#include "test_md_common.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/raid.hpp"

#include "raid/md/md_disk.hpp"
#include "raid/md/md_superblock.hpp"

#define ENABLED_OPTIONS logging, raid1

SISL_LOGGING_INIT(ublk_raid)
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

extern "C" {
struct ublksrv_queue;
extern int ublksrv_queue_send_event(ublksrv_queue const*) { return 0; }
}

using ublkpp::Gi;
using ublkpp::Ki;
using ublkpp::Mi;
using ublkpp::md::test::BufferedDisk;
using ublkpp::md::test::MdSbBuilder;
using ublkpp::md::test::stage_md_sb;

namespace {

// Kept small so the BufferedDisk in-memory backing (and ASan shadow memory) stays modest.
// MdRaid10Factory.SixDiskHappyPath instantiates 6 of these concurrently, which would be
// many GiB of allocation if k_leg_size were realistic.
constexpr uint64_t k_leg_size = 16UL * Mi;
constexpr uint64_t k_default_data_offset_bytes = 1UL * Mi;
constexpr uint32_t k_default_chunk_bytes = 64 * Ki;

std::shared_ptr< BufferedDisk > make_clean_md_leg(std::string id, uint16_t dev_role, uint16_t raid_disks = 4,
                                                  uint64_t events = 42) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, std::move(id));
    MdSbBuilder b{};
    b.raid_disks = raid_disks;
    b.dev_number = dev_role;
    for (uint16_t i = 0; i < raid_disks; ++i)
        b.dev_roles[i] = i;
    b.events = events;
    b.data_offset_sectors = k_default_data_offset_bytes >> ublkpp::SECTOR_SHIFT;
    b.chunksize_sectors = k_default_chunk_bytes >> ublkpp::SECTOR_SHIFT;
    stage_md_sb(*d, b);
    return d;
}

// Stage an md raid 1 (level 1, 2-way mirror) leg. mdadm doesn't use chunk_size or layout for
// pure mirrors, so chunksize_sectors=0 and layout=0 here.
std::shared_ptr< BufferedDisk > make_clean_md1_leg(std::string id, uint16_t dev_role, uint64_t events = 42) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, std::move(id));
    MdSbBuilder b{};
    b.level = 1;
    b.layout = 0;
    b.chunksize_sectors = 0;
    b.raid_disks = 2;
    b.dev_number = dev_role;
    b.dev_roles[0] = 0;
    b.dev_roles[1] = 1;
    b.events = events;
    b.data_offset_sectors = k_default_data_offset_bytes >> ublkpp::SECTOR_SHIFT;
    stage_md_sb(*d, b);
    return d;
}

} // namespace

// -- discovered_topo: rejection paths --------------------------------------------------------

TEST(MdDiscovery, RejectsMissingMdMagic) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    // No SB staged
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsWrongLevel) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.level = 5; // not RAID-1 or RAID-10
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsLevel1NonTwoWayMirror) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.level = 1;
    b.layout = 0;
    b.chunksize_sectors = 0;
    b.raid_disks = 3; // 3-way mirror not supported by ublkpp RAID1
    b.dev_roles[0] = 0;
    b.dev_roles[1] = 1;
    b.dev_roles[2] = 2;
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsFarLayout) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.layout = 0x00020001; // far_copies=2, near_copies=1
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsDirty) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.resync_offset = 0; // not clean
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsReshapeActive) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.feature_map = 0x4; // RESHAPE_ACTIVE
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsRecoveryOffset) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.feature_map = 0x10; // RECOVERY_OFFSET
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsSpareDevRole) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.dev_roles[0] = 0xFFFE; // faulty / spare marker (>= raid_disks)
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsTinyDataOffset) {
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.data_offset_sectors = 8; // 4 KiB; way too small for ublkpp metadata
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsOddDataOffset) {
    // data_offset = 0 is the special signal for unset/1.0; reject explicitly.
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    MdSbBuilder b{};
    b.data_offset_sectors = 0;
    stage_md_sb(*d, b);
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, ublkpp::md::test::default_md_uuid()), std::runtime_error);
}

TEST(MdDiscovery, RejectsWrongVolumeUuid) {
    // md SB carries the default set_uuid {0xa0..0xaf}; caller passes a totally different
    // uuid. MdDisk must refuse to consume the leg (the "this leg doesn't belong to your
    // volume" guard).
    auto d = std::make_shared< BufferedDisk >(k_leg_size, "leg0");
    stage_md_sb(*d, MdSbBuilder{});
    boost::uuids::uuid wrong_uuid{}; // all zeros
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(d, wrong_uuid), std::runtime_error);
}

TEST(MdDiscovery, ReattachRejectsWrongVolumeUuid) {
    auto leaf = make_clean_md_leg("leg0", 0);
    // First attach with the correct uuid succeeds and stamps the MdDisk SB.
    {
        auto md = std::make_shared< ublkpp::md::MdDisk >(leaf, ublkpp::md::test::default_md_uuid());
        ASSERT_NE(md, nullptr);
    }
    // Reattach with a different uuid must fail at the stored-uuid verification step.
    boost::uuids::uuid wrong_uuid{};
    EXPECT_THROW(std::make_shared< ublkpp::md::MdDisk >(leaf, wrong_uuid), std::runtime_error);
}

// -- successful single-leg wrap + topology persistence ---------------------------------------

TEST(MdDiscovery, WrapsCleanLeg) {
    auto leaf = make_clean_md_leg("leg0", /*dev_role=*/1);
    auto md = std::make_shared< ublkpp::md::MdDisk >(leaf, ublkpp::md::test::default_md_uuid());
    auto const& t = md->topology();
    EXPECT_EQ(t.dev_role, 1);
    EXPECT_EQ(t.raid_disks, 4);
    EXPECT_EQ(t.md_chunk_size, k_default_chunk_bytes);
    EXPECT_EQ(t.data_offset_bytes, k_default_data_offset_bytes);
    EXPECT_EQ(t.layout_near, 2);
    EXPECT_EQ(t.layout_far, 0);
    // layout_far_offset is captured verbatim from md SB; mdadm sets bit 16 even when
    // far_copies=0 (default near=2 layout = 0x00010002), so this can be 0 or 1 depending
    // on which mdadm wrote the array. Don't assert on it.
    EXPECT_EQ(t.events, 42);
}

TEST(MdDiscovery, ScrubsMdSbAfterWrap) {
    auto leaf = make_clean_md_leg("leg0", 0);
    // Pre-import: md magic at byte 4 KiB
    EXPECT_EQ(leaf->data()[4 * Ki], 0xfc); // 0xa92b4efc LE first byte
    {
        auto md = std::make_shared< ublkpp::md::MdDisk >(leaf, ublkpp::md::test::default_md_uuid());
        ASSERT_NE(md, nullptr);
    }
    // Post-import: md SB must be zeroed; MdDisk magic must be at byte 0.
    for (size_t i = 0; i < 4 * Ki; ++i)
        EXPECT_EQ(leaf->data()[4 * Ki + i], 0u) << "byte " << i;
    EXPECT_EQ(std::memcmp(leaf->data(), ublkpp::md::k_magic, ublkpp::md::k_magic_size), 0);
}

TEST(MdDiscovery, ReattachReadsOwnSbWithoutMdSb) {
    auto leaf = make_clean_md_leg("leg0", /*dev_role=*/2);
    {
        auto md1 = std::make_shared< ublkpp::md::MdDisk >(leaf, ublkpp::md::test::default_md_uuid());
        ASSERT_NE(md1, nullptr);
    }
    // First wrap scrubbed md SB. Second wrap must succeed by reading our own SB at byte 0.
    auto md2 = std::make_shared< ublkpp::md::MdDisk >(leaf, ublkpp::md::test::default_md_uuid());
    EXPECT_EQ(md2->topology().dev_role, 2);
    EXPECT_EQ(md2->topology().raid_disks, 4);
}

// -- piecewise translation -------------------------------------------------------------------

TEST(MdTranslation, HeadZoneShiftsByOnePage) {
    auto leaf = make_clean_md_leg("leg0", 0);
    auto md = std::make_shared< ublkpp::md::MdDisk >(leaf, ublkpp::md::test::default_md_uuid());

    // Write a distinctive byte at upper offset 0; expect it at leaf byte 4 KiB.
    uint8_t pattern[ublkpp::md::k_page_size];
    std::memset(pattern, 0x5a, sizeof(pattern));
    iovec iov{.iov_base = pattern, .iov_len = sizeof(pattern)};
    auto r = md->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0);
    ASSERT_TRUE(r) << r.error().message();
    EXPECT_EQ(leaf->data()[4 * Ki], 0x5a);
    EXPECT_EQ(leaf->data()[4 * Ki + 4095], 0x5a);
    // MdDisk SB still intact at byte 0
    EXPECT_EQ(std::memcmp(leaf->data(), ublkpp::md::k_magic, ublkpp::md::k_magic_size), 0);
}

TEST(MdTranslation, DataBandLandsAtMdDataOffset) {
    auto leaf = make_clean_md_leg("leg0", 0);

    // Pre-stage user data at the md data_offset on the leaf (simulate bytes md wrote).
    constexpr uint64_t k_pattern_seed = 0xDEADBEEFCAFEBABEULL;
    constexpr size_t k_pattern_len = 64 * Ki;
    ublkpp::md::test::fill_pattern(leaf->data() + k_default_data_offset_bytes, k_pattern_len, k_pattern_seed);

    auto md = std::make_shared< ublkpp::md::MdDisk >(leaf, ublkpp::md::test::default_md_uuid());

    // The first byte of "user data" from MdDisk's view is at upper offset
    // _data_band_threshold. Read 64 KiB starting there and compare with the leaf bytes
    // at data_offset.
    auto const upper_user_start = md->data_band_threshold();
    // Sanity: upper_user_start should equal _data_band_threshold (R + chunk).
    // We can't read R from outside, but we can verify the read returns md's bytes.
    std::vector< uint8_t > read_buf(k_pattern_len, 0);
    iovec iov{.iov_base = read_buf.data(), .iov_len = k_pattern_len};
    auto r = md->sync_iov(UBLK_IO_OP_READ, &iov, 1, static_cast< off_t >(upper_user_start));
    ASSERT_TRUE(r) << r.error().message();
    EXPECT_EQ(std::memcmp(read_buf.data(), leaf->data() + k_default_data_offset_bytes, k_pattern_len), 0);
}

// -- factory: make_md_raid1_disk ----------------------------------------------------------

TEST(MdRaid1Factory, AcceptsComplementaryPair) {
    auto a = make_clean_md_leg("leg0", 0);
    auto b = make_clean_md_leg("leg1", 1);
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    auto raid1 = ublkpp::md::make_md_raid1_disk(uuid, {a, b});
    EXPECT_NE(raid1, nullptr);
}

TEST(MdRaid1Factory, AcceptsLevel1Array) {
    // Pure md level 1, no chunking, no layout. The factory must accept this just like a
    // 2-leg near=2 md raid 10.
    auto a = make_clean_md1_leg("md1_leg0", 0);
    auto b = make_clean_md1_leg("md1_leg1", 1);
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    auto raid1 = ublkpp::md::make_md_raid1_disk(uuid, {a, b});
    EXPECT_NE(raid1, nullptr);
}

TEST(MdRaid1Factory, RejectsMixedLevels) {
    // One leg from md level 1, one from md level 10 - explicit cross-leg level mismatch.
    auto a = make_clean_md_leg("leg0_l10", 0);
    auto b = make_clean_md1_leg("leg1_l1", 1);
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    EXPECT_THROW(ublkpp::md::make_md_raid1_disk(uuid, {a, b}), std::runtime_error);
}

TEST(MdRaid1Factory, RejectsEventsMismatch) {
    auto a = make_clean_md_leg("leg0", 0, 4, /*events=*/42);
    auto b = make_clean_md_leg("leg1", 1, 4, /*events=*/43);
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    EXPECT_THROW(ublkpp::md::make_md_raid1_disk(uuid, {a, b}), std::runtime_error);
}

TEST(MdRaid1Factory, RejectsNonComplementaryRoles) {
    auto a = make_clean_md_leg("leg0", 0); // dev_role 0 (pair 0)
    auto b = make_clean_md_leg("leg1", 2); // dev_role 2 (pair 1) - not pair 0's mate
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    EXPECT_THROW(ublkpp::md::make_md_raid1_disk(uuid, {a, b}), std::runtime_error);
}

TEST(MdRaid1Factory, ReordersPair) {
    // Provide legs in (1, 0) order; factory should swap so 0 is "a".
    auto leg0 = make_clean_md_leg("leg0", 0);
    auto leg1 = make_clean_md_leg("leg1", 1);
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    auto raid1 = ublkpp::md::make_md_raid1_disk(uuid, {leg1, leg0}); // swapped
    EXPECT_NE(raid1, nullptr);
}

// -- factory: make_md_raid10_disk ------------------------------------------------------------

TEST(MdRaid10Factory, FourDiskHappyPath) {
    std::vector< ublkpp::disk_handle > legs;
    for (uint16_t i = 0; i < 4; ++i)
        legs.push_back(make_clean_md_leg(fmt::format("leg{}", i), i, 4));
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    auto stack = ublkpp::md::make_md_raid10_disk(uuid, std::move(legs));
    EXPECT_NE(stack, nullptr);
}

TEST(MdRaid10Factory, FourDiskShuffledOrder) {
    // Provide legs in order (3, 1, 0, 2); factory should rebuild pairs (0,1) and (2,3).
    std::vector< ublkpp::disk_handle > legs;
    legs.push_back(make_clean_md_leg("leg3", 3, 4));
    legs.push_back(make_clean_md_leg("leg1", 1, 4));
    legs.push_back(make_clean_md_leg("leg0", 0, 4));
    legs.push_back(make_clean_md_leg("leg2", 2, 4));
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    auto stack = ublkpp::md::make_md_raid10_disk(uuid, std::move(legs));
    EXPECT_NE(stack, nullptr);
}

TEST(MdRaid10Factory, SixDiskHappyPath) {
    std::vector< ublkpp::disk_handle > legs;
    for (uint16_t i = 0; i < 6; ++i)
        legs.push_back(make_clean_md_leg(fmt::format("leg{}", i), i, 6));
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    auto stack = ublkpp::md::make_md_raid10_disk(uuid, std::move(legs));
    EXPECT_NE(stack, nullptr);
}

TEST(MdRaid10Factory, RejectsLevel1Source) {
    // Building a RAID10 from md level 1 legs makes no sense (no striping in the source);
    // factory should refuse with a clear error rather than silently treating it as raid 10.
    std::vector< ublkpp::disk_handle > legs;
    legs.push_back(make_clean_md1_leg("md1_leg0", 0));
    legs.push_back(make_clean_md1_leg("md1_leg1", 1));
    legs.push_back(make_clean_md1_leg("md1_leg2", 0));
    legs.push_back(make_clean_md1_leg("md1_leg3", 1));
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    EXPECT_THROW(ublkpp::md::make_md_raid10_disk(uuid, std::move(legs)), std::runtime_error);
}

TEST(MdRaid10Factory, RejectsRaidDisksMismatch) {
    // Three legs claiming raid_disks=4 (not enough)
    std::vector< ublkpp::disk_handle > legs;
    for (uint16_t i = 0; i < 3; ++i)
        legs.push_back(make_clean_md_leg(fmt::format("leg{}", i), i, 4));
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    EXPECT_THROW(ublkpp::md::make_md_raid10_disk(uuid, std::move(legs)), std::runtime_error);
}

TEST(MdRaid10Factory, RejectsDuplicateDevRole) {
    // Two legs both claim role 0
    std::vector< ublkpp::disk_handle > legs;
    legs.push_back(make_clean_md_leg("leg0", 0, 4));
    legs.push_back(make_clean_md_leg("leg0_dup", 0, 4));
    legs.push_back(make_clean_md_leg("leg2", 2, 4));
    legs.push_back(make_clean_md_leg("leg3", 3, 4));
    auto const& uuid = ublkpp::md::test::default_md_uuid();
    EXPECT_THROW(ublkpp::md::make_md_raid10_disk(uuid, std::move(legs)), std::runtime_error);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    return RUN_ALL_TESTS();
}
