#include "test_raid1_common.hpp"

TEST(Raid1, PickSuper) {
    {
        auto deva_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 0}},
            ._reserved = {0}};
        auto devb_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 0}},
            ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &deva_sb);
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::EITHER);
    }
    {
        auto deva_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 0}},
            ._reserved = {0}};
        auto devb_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 1}},
            ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &devb_sb);
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::DEVB);
    }
    {
        auto deva_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 1}},
            ._reserved = {0}};
        auto devb_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 1, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 1}},
            ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &devb_sb);
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::EITHER);
    }
    {
        auto deva_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 2}},
            ._reserved = {0}};
        auto devb_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 1, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 1}},
            ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &deva_sb);
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::DEVA);
    }
    {
        auto deva_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 1, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 2}},
            ._reserved = {0}};
        auto devb_sb = ublkpp::raid1::SuperBlock{
            .header = {.magic = {0}, .version = 0, .uuid = {0}},
            .fields = {.clean_unmount = 0, .read_route = 0, .bitmap = {.chunk_size = 0, .age = 2}},
            ._reserved = {0}};
        auto choice = ublkpp::raid1::pick_superblock(&deva_sb, &devb_sb);
        EXPECT_EQ(choice, &deva_sb);
        EXPECT_EQ((ublkpp::raid1::read_route)choice->fields.read_route, ublkpp::raid1::read_route::EITHER);
    }
}
