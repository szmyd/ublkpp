#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <vector>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv.h>

#include "ublkpp/drivers/fs_disk.hpp"
#include "ublkpp/lib/common.hpp"

SISL_LOGGING_INIT(ublk_drivers)

SISL_OPTIONS_ENABLE(logging, fs_disk)

namespace {

// Helper to allocate aligned memory for O_DIRECT
void* aligned_alloc_helper(size_t alignment, size_t size) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) {
        return nullptr;
    }
    return ptr;
}

class AlignedBuffer {
    void* ptr_;
    size_t size_;
public:
    AlignedBuffer(size_t size, size_t alignment = 512) : ptr_(nullptr), size_(size) {
        ptr_ = aligned_alloc_helper(alignment, size);
        if (ptr_) {
            memset(ptr_, 0, size);
        }
    }
    ~AlignedBuffer() {
        if (ptr_) free(ptr_);
    }
    void* get() { return ptr_; }
    size_t size() const { return size_; }
    uint8_t* data() { return static_cast<uint8_t*>(ptr_); }

    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;
};

class FSDiskTest : public ::testing::Test {
protected:
    std::filesystem::path test_file_path;
    static constexpr size_t TEST_FILE_SIZE = 16 * 1024 * 1024; // 16 MB

    void SetUp() override {
        // Create a temporary test file
        auto temp_template = std::filesystem::temp_directory_path() / "test_fsdisk_XXXXXX";
        std::string temp_str = temp_template.string();

        // mkstemp requires a mutable C string
        std::vector<char> temp_chars(temp_str.begin(), temp_str.end());
        temp_chars.push_back('\0');

        int fd = mkstemp(temp_chars.data());
        ASSERT_GE(fd, 0) << "Failed to create temporary file: " << strerror(errno);
        test_file_path = std::string(temp_chars.data());

        // Resize the file to TEST_FILE_SIZE
        ASSERT_EQ(ftruncate(fd, TEST_FILE_SIZE), 0) << "Failed to resize temporary file: " << strerror(errno);
        close(fd);
    }

    void TearDown() override {
        // Clean up the temporary file
        if (std::filesystem::exists(test_file_path)) {
            std::filesystem::remove(test_file_path);
        }
    }
};

// Test: Constructor with valid regular file
TEST_F(FSDiskTest, ConstructorValidFile) {
    ASSERT_NO_THROW({
        auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);
        EXPECT_EQ(disk->id(), test_file_path.native());
        EXPECT_GT(disk->capacity(), 0);
        EXPECT_GT(disk->block_size(), 0);
    });
}

// Test: Constructor with non-existent file
TEST(FSDiskConstructor, NonExistentFile) {
    auto non_existent_path = std::filesystem::temp_directory_path() / "non_existent_file_that_does_not_exist_12345";
    EXPECT_THROW({
        auto disk = std::make_unique<ublkpp::FSDisk>(non_existent_path);
    }, std::runtime_error);
}

// Test: Capacity calculation for regular files
TEST_F(FSDiskTest, CapacityCalculation) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    // Capacity should be aligned to max_sectors
    uint64_t expected_sectors = TEST_FILE_SIZE >> ublkpp::SECTOR_SHIFT;
    auto params = disk->params();
    expected_sectors -= (expected_sectors % params->basic.max_sectors);
    uint64_t expected_capacity = expected_sectors << ublkpp::SECTOR_SHIFT;

    EXPECT_EQ(disk->capacity(), expected_capacity);
}

// Test: Block size parameters
TEST_F(FSDiskTest, BlockSizeParameters) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    auto params = disk->params();
    uint32_t logical_bs = 1U << params->basic.logical_bs_shift;
    uint32_t physical_bs = 1U << params->basic.physical_bs_shift;

    EXPECT_GT(logical_bs, 0);
    EXPECT_GT(physical_bs, 0);
    EXPECT_EQ(disk->block_size(), logical_bs);
}

// Test: Discard capability for regular files
TEST_F(FSDiskTest, DiscardCapability) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    // Regular files should support discard
    EXPECT_TRUE(disk->can_discard());
    EXPECT_TRUE(disk->params()->types & UBLK_PARAM_TYPE_DISCARD);
}

// Test: sync_iov read operation
TEST_F(FSDiskTest, SyncReadOperation) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    // Use aligned buffer and size for O_DIRECT compatibility
    size_t block_size = disk->block_size();
    AlignedBuffer write_buf(block_size);

    // Fill with test pattern
    for (size_t i = 0; i < block_size; ++i) {
        write_buf.data()[i] = static_cast<uint8_t>(i & 0xFF);
    }

    iovec write_iov;
    write_iov.iov_base = write_buf.get();
    write_iov.iov_len = block_size;

    auto write_result = disk->sync_iov(UBLK_IO_OP_WRITE, &write_iov, 1, 0);
    ASSERT_TRUE(write_result.has_value());

    // Now read it back
    AlignedBuffer read_buf(block_size);
    iovec read_iov;
    read_iov.iov_base = read_buf.get();
    read_iov.iov_len = block_size;

    auto result = disk->sync_iov(UBLK_IO_OP_READ, &read_iov, 1, 0);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), block_size);
    EXPECT_EQ(0, memcmp(write_buf.get(), read_buf.get(), block_size));
}

// Test: sync_iov write operation
TEST_F(FSDiskTest, SyncWriteOperation) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    size_t block_size = disk->block_size();
    AlignedBuffer write_buf(block_size);

    // Fill with test pattern
    memset(write_buf.get(), 0xAB, block_size);

    iovec iov;
    iov.iov_base = write_buf.get();
    iov.iov_len = block_size;

    auto write_result = disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, block_size);

    ASSERT_TRUE(write_result.has_value());
    EXPECT_EQ(write_result.value(), block_size);

    // Verify data by reading back through the disk
    AlignedBuffer read_buf(block_size);
    iov.iov_base = read_buf.get();
    iov.iov_len = block_size;

    auto read_result = disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, block_size);
    ASSERT_TRUE(read_result.has_value());
    EXPECT_EQ(0, memcmp(write_buf.get(), read_buf.get(), block_size));
}

// Test: sync_iov with multiple iovecs (vectored I/O)
TEST_F(FSDiskTest, SyncVectoredRead) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    size_t block_size = disk->block_size();
    // Use two blocks for vectored I/O
    AlignedBuffer buf1(block_size);
    AlignedBuffer buf2(block_size);

    // Fill with different patterns
    memset(buf1.get(), 0x11, block_size);
    memset(buf2.get(), 0x22, block_size);

    // Write test data using vectored I/O
    iovec write_iovs[2];
    write_iovs[0].iov_base = buf1.get();
    write_iovs[0].iov_len = block_size;
    write_iovs[1].iov_base = buf2.get();
    write_iovs[1].iov_len = block_size;

    auto write_result = disk->sync_iov(UBLK_IO_OP_WRITE, write_iovs, 2, 0);
    ASSERT_TRUE(write_result.has_value());

    // Read using multiple iovecs
    AlignedBuffer read_buf1(block_size);
    AlignedBuffer read_buf2(block_size);
    iovec read_iovs[2];
    read_iovs[0].iov_base = read_buf1.get();
    read_iovs[0].iov_len = block_size;
    read_iovs[1].iov_base = read_buf2.get();
    read_iovs[1].iov_len = block_size;

    auto result = disk->sync_iov(UBLK_IO_OP_READ, read_iovs, 2, 0);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 2 * block_size);
    EXPECT_EQ(0, memcmp(buf1.get(), read_buf1.get(), block_size));
    EXPECT_EQ(0, memcmp(buf2.get(), read_buf2.get(), block_size));
}

// Test: sync_iov with invalid operation
TEST_F(FSDiskTest, SyncInvalidOperation) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    std::vector<uint8_t> buffer(8);
    iovec iov;
    iov.iov_base = buffer.data();
    iov.iov_len = buffer.size();

    // Use an invalid operation code
    auto result = disk->sync_iov(0xFF, &iov, 1, 0);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::invalid_argument));
}

// Test: Reading/writing at various alignments
TEST_F(FSDiskTest, UnalignedAccess) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    size_t block_size = disk->block_size();

    // Test multiple block-aligned offsets
    std::vector<off_t> offsets = {
        0,
        static_cast<off_t>(block_size),
        static_cast<off_t>(2 * block_size),
        static_cast<off_t>(4 * block_size),
        static_cast<off_t>(8 * block_size)
    };

    for (off_t offset : offsets) {
        AlignedBuffer write_buf(block_size);
        // Fill with pattern based on offset
        memset(write_buf.get(), static_cast<int>(offset / block_size), block_size);

        iovec iov;
        iov.iov_base = write_buf.get();
        iov.iov_len = block_size;

        auto write_result = disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, offset);
        ASSERT_TRUE(write_result.has_value()) << "Failed to write at offset " << offset;

        AlignedBuffer read_buf(block_size);
        iov.iov_base = read_buf.get();
        auto read_result = disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, offset);
        ASSERT_TRUE(read_result.has_value()) << "Failed to read at offset " << offset;

        EXPECT_EQ(0, memcmp(write_buf.get(), read_buf.get(), block_size))
            << "Data mismatch at offset " << offset;
    }
}

// Test: Large I/O operations
TEST_F(FSDiskTest, LargeIO) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    // Write 1MB of data
    size_t large_size = 1024 * 1024;
    AlignedBuffer write_buf(large_size);

    // Fill with pattern
    for (size_t i = 0; i < large_size; ++i) {
        write_buf.data()[i] = static_cast<uint8_t>(i & 0xFF);
    }

    iovec iov;
    iov.iov_base = write_buf.get();
    iov.iov_len = large_size;

    auto write_result = disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0);
    ASSERT_TRUE(write_result.has_value());

    // Read back
    AlignedBuffer read_buf(large_size);
    iov.iov_base = read_buf.get();
    auto read_result = disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, 0);
    ASSERT_TRUE(read_result.has_value());

    EXPECT_EQ(0, memcmp(write_buf.get(), read_buf.get(), large_size));
}

// Test: ID returns correct path
TEST_F(FSDiskTest, IDMethod) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);
    EXPECT_EQ(disk->id(), test_file_path.native());
}

// Test: Multiple read/write cycles
TEST_F(FSDiskTest, MultipleReadWriteCycles) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    size_t block_size = disk->block_size();

    for (int cycle = 0; cycle < 10; ++cycle) {
        AlignedBuffer write_buf(block_size);

        for (size_t i = 0; i < block_size; ++i) {
            write_buf.data()[i] = static_cast<uint8_t>((cycle + i) & 0xFF);
        }

        off_t offset = cycle * block_size;
        iovec iov;
        iov.iov_base = write_buf.get();
        iov.iov_len = block_size;

        auto write_result = disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, offset);
        ASSERT_TRUE(write_result.has_value());

        AlignedBuffer read_buf(block_size);
        iov.iov_base = read_buf.get();
        auto read_result = disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, offset);
        ASSERT_TRUE(read_result.has_value());

        EXPECT_EQ(0, memcmp(write_buf.get(), read_buf.get(), block_size));
    }
}

// Test: Zero-length I/O
TEST_F(FSDiskTest, ZeroLengthIO) {
    auto disk = std::make_unique<ublkpp::FSDisk>(test_file_path);

    iovec iov;
    iov.iov_base = nullptr;
    iov.iov_len = 0;

    auto result = disk->sync_iov(UBLK_IO_OP_READ, &iov, 0, 0);

    // The behavior may vary, but it should not crash
    // Typically returns 0 bytes read
    if (result.has_value()) {
        EXPECT_EQ(result.value(), 0);
    }
}

} // anonymous namespace

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, fs_disk);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
