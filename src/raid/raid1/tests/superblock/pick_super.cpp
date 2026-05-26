#include "test_raid1_common.hpp"

TEST(Raid1, PickSuper) {
    // Case 1: Both superblocks invalid (age=0, no magic). pick_superblock returns dev_a unchanged.
    // Use explicit non-zero read_route values in both structs so the EITHER assertion is
    // non-vacuous: pick_superblock must not alter the route when ages are equal, confirming
    // it preserved dev_a's existing route value exactly as-is.
    {
        auto deva_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0,
                       .read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::EITHER),
                       .device_b = 0,
                       .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = 0}},
            .superbitmap_reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0,
                                                 .read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVB),
                                                 .device_b = 0,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = 0}},
                                      .superbitmap_reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &deva_sb);
        // pick_superblock returns dev_a unchanged when ages are equal; dev_a has EITHER.
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::EITHER);
        // dev_b's route must not have been modified.
        EXPECT_EQ((ublkpp::raid1::read_route)devb_sb.fields.read_route, ublkpp::raid1::read_route::DEVB);
    }
    // Case 2: dev_b has higher age → pick_superblock returns dev_b and sets read_route=DEVB.
    // Verifies that the code explicitly sets DEVB (non-vacuous: initial value is EITHER=0).
    {
        auto deva_sb = ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                                 .fields = {.clean_unmount = 0,
                                                            .read_route = 0,
                                                            .device_b = 0,
                                                            .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = 0}},
                                                 .superbitmap_reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0,
                                                 .read_route = 0,
                                                 .device_b = 1,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(1)}},
                                      .superbitmap_reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &devb_sb);
        // pick_superblock must write DEVB into the returned superblock.
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::DEVB);
    }
    // Case 3: Equal ages, dev_b is clean, dev_a is unclean → pick_superblock routes to B.
    // pick_superblock must write DEVB so __init_bitmap_and_degraded_route opens the array
    // in degraded mode; without this the array opens healthy and A's in-flight writes are lost.
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0,
                                                 .read_route = 0,
                                                 .device_b = 0,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(1)}},
                                      .superbitmap_reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 1,
                                                 .read_route = 0,
                                                 .device_b = 1,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(1)}},
                                      .superbitmap_reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &devb_sb);
        // Must write DEVB so the degraded init path fires on next open.
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::DEVB);
    }
    // Case 4: dev_a has higher age → pick_superblock returns dev_a and sets read_route=DEVA.
    // Verifies that the code explicitly sets DEVA (non-vacuous: initial value is EITHER=0).
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0,
                                                 .read_route = 0,
                                                 .device_b = 0,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(2)}},
                                      .superbitmap_reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 1,
                                                 .read_route = 0,
                                                 .device_b = 1,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(1)}},
                                      .superbitmap_reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &deva_sb);
        // pick_superblock must write DEVA into the returned superblock.
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::DEVA);
    }
    // Case 5: Equal ages, dev_a is clean, dev_b is unclean → pick_superblock routes to A.
    // pick_superblock must write DEVA so __init_bitmap_and_degraded_route opens the array
    // in degraded mode; without this the array opens healthy and B's in-flight writes are lost.
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 1,
                                                 .read_route = 0,
                                                 .device_b = 0,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(2)}},
                                      .superbitmap_reserved = {0}};
        auto devb_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0,
                                                 .read_route = 0,
                                                 .device_b = 1,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(2)}},
                                      .superbitmap_reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &deva_sb);
        // Must write DEVA so the degraded init path fires on next open.
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::DEVA);
    }
    // Case 6: Explicit DEVA check — dev_a has higher age and initial route DEVB to confirm
    // pick_superblock overwrites the field to DEVA (not just preserving the initial value).
    {
        auto deva_sb =
            ublkpp::raid1::SuperBlock{.header = {.magic = {0}, .version = 0, .uuid = {0}},
                                      .fields = {.clean_unmount = 0,
                                                 .read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVB),
                                                 .device_b = 0,
                                                 .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(5)}},
                                      .superbitmap_reserved = {0}};
        auto devb_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0,
                       .read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::EITHER),
                       .device_b = 1,
                       .bitmap = {._reserved = {0x00}, .chunk_size = 0, .age = htobe64(3)}},
            .superbitmap_reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &deva_sb);
        // Must be DEVA even though dev_a was initialised with DEVB — verifies the code
        // actively writes DEVA and does not merely return the pre-existing field.
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::DEVA);
    }
}
