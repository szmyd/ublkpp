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

