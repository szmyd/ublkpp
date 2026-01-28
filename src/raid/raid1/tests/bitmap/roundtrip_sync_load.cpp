#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <map>

#include "tests/test_disk.hpp"
#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_superblock.hpp"

using ::testing::_;

// Test full round-trip: dirty -> sync_to -> load_from -> verify
TEST(Raid1BitmapRoundtrip, BasicSyncAndLoad) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 8 * ublkpp::Gi});

    // Storage for written pages
    std::map< off_t, std::shared_ptr< uint8_t[] > > written_pages;

    // Phase 1: Create bitmap and dirty some regions
    auto bitmap1 = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    auto page_width = 32 * ublkpp::Ki * 4 * ublkpp::Ki * 8UL;
    bitmap1.dirty_region(0, page_width);              // Page 0
    bitmap1.dirty_region(2 * page_width, page_width); // Page 2

    // Phase 2: sync_to (write to "disk")
    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillRepeatedly([&written_pages](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            // Store written data
            for (uint32_t i = 0; i < nr_vecs; ++i) {
                auto page_data = std::shared_ptr< uint8_t[] >(new uint8_t[iovecs[i].iov_len]);
                std::memcpy(page_data.get(), iovecs[i].iov_base, iovecs[i].iov_len);
                written_pages[addr + (i * iovecs[i].iov_len)] = page_data;
            }
            return nr_vecs * ublkpp::raid1::Bitmap::page_size();
        });

    auto sync_res = bitmap1.sync_to(*device, ublkpp::raid1::Bitmap::page_size());
    EXPECT_TRUE(sync_res);
    EXPECT_EQ(2UL, written_pages.size()); // 2 non-consecutive pages

    // Phase 3: load_from (read from "disk")
    auto bitmap2 = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillRepeatedly([&written_pages](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_EQ(1U, nr_vecs);

            auto it = written_pages.find(addr);
            if (it != written_pages.end()) {
                // Found a page that was written - copy it
                std::memcpy(iovecs->iov_base, it->second.get(), iovecs->iov_len);
            } else {
                // Page not written - return zeros
                std::memset(iovecs->iov_base, 0, iovecs->iov_len);
            }
            return ublkpp::raid1::Bitmap::page_size();
        });

    bitmap2.load_from(*device);

    // Phase 4: Verify loaded bitmap matches original
    EXPECT_TRUE(bitmap2.is_dirty(0, page_width));              // Page 0 should be dirty
    EXPECT_FALSE(bitmap2.is_dirty(1 * page_width, page_width)); // Page 1 should be clean
    EXPECT_TRUE(bitmap2.is_dirty(2 * page_width, page_width)); // Page 2 should be dirty

    // Also check dirty count
    EXPECT_EQ(2UL, bitmap2.dirty_pages());
}

// Test round-trip with many pages and batching
TEST(Raid1BitmapRoundtrip, LargeBitmapWithBatching) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 32 * ublkpp::Gi});

    std::map< off_t, std::shared_ptr< uint8_t[] > > written_pages;
    auto page_width = 32 * ublkpp::Ki * 4 * ublkpp::Ki * 8UL;

    // Phase 1: Dirty many consecutive pages
    auto bitmap1 = ublkpp::raid1::Bitmap(32 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    // Dirty pages 0-9 (10 consecutive pages)
    bitmap1.dirty_region(0, 10 * page_width);

    // Phase 2: sync_to with batching
    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillRepeatedly([&written_pages](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            // Store all batched pages
            for (uint32_t i = 0; i < nr_vecs; ++i) {
                auto page_data = std::shared_ptr< uint8_t[] >(new uint8_t[iovecs[i].iov_len]);
                std::memcpy(page_data.get(), iovecs[i].iov_base, iovecs[i].iov_len);
                written_pages[addr + (i * iovecs[i].iov_len)] = page_data;
            }
            return nr_vecs * ublkpp::raid1::Bitmap::page_size();
        });

    auto sync_res = bitmap1.sync_to(*device, ublkpp::raid1::Bitmap::page_size());
    EXPECT_TRUE(sync_res);
    EXPECT_EQ(10UL, written_pages.size());

    // Phase 3: Load into new bitmap
    auto bitmap2 = ublkpp::raid1::Bitmap(32 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillRepeatedly([&written_pages](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> ublkpp::io_result {
            auto it = written_pages.find(addr);
            if (it != written_pages.end()) {
                std::memcpy(iovecs->iov_base, it->second.get(), iovecs->iov_len);
            } else {
                std::memset(iovecs->iov_base, 0, iovecs->iov_len);
            }
            return ublkpp::raid1::Bitmap::page_size();
        });

    bitmap2.load_from(*device);

    // Phase 4: Verify all 10 pages are dirty
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(bitmap2.is_dirty(i * page_width, page_width))
            << "Page " << i << " should be dirty";
    }
    EXPECT_FALSE(bitmap2.is_dirty(10 * page_width, page_width)); // Page 10 should be clean

    EXPECT_EQ(10UL, bitmap2.dirty_pages());
}

// Test round-trip with modifications after load
TEST(Raid1BitmapRoundtrip, ModifyAfterLoad) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 8 * ublkpp::Gi});

    std::map< off_t, std::shared_ptr< uint8_t[] > > storage;
    auto page_width = 32 * ublkpp::Ki * 4 * ublkpp::Ki * 8UL;

    // Round 1: Create and sync
    {
        auto bitmap = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);
        bitmap.dirty_region(0, page_width); // Page 0

        EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .WillRepeatedly([&storage](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
                for (uint32_t i = 0; i < nr_vecs; ++i) {
                    auto page_data = std::shared_ptr< uint8_t[] >(new uint8_t[iovecs[i].iov_len]);
                    std::memcpy(page_data.get(), iovecs[i].iov_base, iovecs[i].iov_len);
                    storage[addr + (i * iovecs[i].iov_len)] = page_data;
                }
                return nr_vecs * ublkpp::raid1::Bitmap::page_size();
            });

        bitmap.sync_to(*device, ublkpp::raid1::Bitmap::page_size());
    }

    // Round 2: Load, modify, and sync again
    {
        auto bitmap = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

        EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
            .WillRepeatedly([&storage](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> ublkpp::io_result {
                auto it = storage.find(addr);
                if (it != storage.end()) {
                    std::memcpy(iovecs->iov_base, it->second.get(), iovecs->iov_len);
                } else {
                    std::memset(iovecs->iov_base, 0, iovecs->iov_len);
                }
                return ublkpp::raid1::Bitmap::page_size();
            });

        bitmap.load_from(*device);

        // Verify page 0 is dirty from previous round
        EXPECT_TRUE(bitmap.is_dirty(0, page_width));

        // Modify: dirty page 1
        bitmap.dirty_region(1 * page_width, page_width);

        // Sync again
        EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .WillRepeatedly([&storage](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
                for (uint32_t i = 0; i < nr_vecs; ++i) {
                    auto page_data = std::shared_ptr< uint8_t[] >(new uint8_t[iovecs[i].iov_len]);
                    std::memcpy(page_data.get(), iovecs[i].iov_base, iovecs[i].iov_len);
                    storage[addr + (i * iovecs[i].iov_len)] = page_data;
                }
                return nr_vecs * ublkpp::raid1::Bitmap::page_size();
            });

        bitmap.sync_to(*device, ublkpp::raid1::Bitmap::page_size());
    }

    // Round 3: Load and verify both pages are dirty
    {
        auto bitmap = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

        EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
            .WillRepeatedly([&storage](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> ublkpp::io_result {
                auto it = storage.find(addr);
                if (it != storage.end()) {
                    std::memcpy(iovecs->iov_base, it->second.get(), iovecs->iov_len);
                } else {
                    std::memset(iovecs->iov_base, 0, iovecs->iov_len);
                }
                return ublkpp::raid1::Bitmap::page_size();
            });

        bitmap.load_from(*device);

        EXPECT_TRUE(bitmap.is_dirty(0, page_width)); // Page 0 (from round 1)
        EXPECT_TRUE(bitmap.is_dirty(1 * page_width, page_width)); // Page 1 (from round 2)
        EXPECT_EQ(2UL, bitmap.dirty_pages());
    }
}

// Test that cleaned regions don't persist
TEST(Raid1BitmapRoundtrip, CleanedRegionsDontPersist) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 8 * ublkpp::Gi});

    std::map< off_t, std::shared_ptr< uint8_t[] > > storage;
    auto page_width = 32 * ublkpp::Ki * 4 * ublkpp::Ki * 8UL;

    // Phase 1: Dirty then clean
    auto bitmap1 = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);
    bitmap1.dirty_region(0, page_width);
    bitmap1.clean_region(0, page_width); // Clean it immediately

    // Phase 2: sync_to should not write zero pages
    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(0); // No writes expected!

    auto sync_res = bitmap1.sync_to(*device, ublkpp::raid1::Bitmap::page_size());
    EXPECT_TRUE(sync_res);

    // Phase 3: Load should find no dirty pages
    auto bitmap2 = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> ublkpp::io_result {
            std::memset(iovecs->iov_base, 0, iovecs->iov_len);
            return ublkpp::raid1::Bitmap::page_size();
        });

    bitmap2.load_from(*device);

    EXPECT_FALSE(bitmap2.is_dirty(0, page_width));
    EXPECT_EQ(0UL, bitmap2.dirty_pages());
}

// Test offset parameter in round-trip
TEST(Raid1BitmapRoundtrip, WithSuperBlockOffset) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 8 * ublkpp::Gi});

    std::map< off_t, std::shared_ptr< uint8_t[] > > storage;
    auto page_width = 32 * ublkpp::Ki * 4 * ublkpp::Ki * 8UL;
    constexpr auto OFFSET = 4096UL; // sizeof(SuperBlock)

    // Phase 1: Dirty and sync with offset
    auto bitmap1 = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);
    bitmap1.dirty_region(0, page_width);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&storage, OFFSET](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            // Verify address includes offset
            EXPECT_EQ(OFFSET, addr);

            for (uint32_t i = 0; i < nr_vecs; ++i) {
                auto page_data = std::shared_ptr< uint8_t[] >(new uint8_t[iovecs[i].iov_len]);
                std::memcpy(page_data.get(), iovecs[i].iov_base, iovecs[i].iov_len);
                storage[addr + (i * iovecs[i].iov_len)] = page_data;
            }
            return nr_vecs * ublkpp::raid1::Bitmap::page_size();
        });

    bitmap1.sync_to(*device, OFFSET);

    // Phase 2: Load (without offset - load_from always uses sizeof(SuperBlock))
    auto bitmap2 = ublkpp::raid1::Bitmap(8 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillRepeatedly([&storage](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> ublkpp::io_result {
            auto it = storage.find(addr);
            if (it != storage.end()) {
                std::memcpy(iovecs->iov_base, it->second.get(), iovecs->iov_len);
            } else {
                std::memset(iovecs->iov_base, 0, iovecs->iov_len);
            }
            return ublkpp::raid1::Bitmap::page_size();
        });

    bitmap2.load_from(*device);

    // Verify page loaded correctly
    EXPECT_TRUE(bitmap2.is_dirty(0, page_width));
    EXPECT_EQ(1UL, bitmap2.dirty_pages());
}
