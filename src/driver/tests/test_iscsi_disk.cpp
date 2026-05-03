#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv.h>

#include "ublkpp/drivers/iscsi_disk.hpp"
#include "ublkpp/lib/common.hpp"

SISL_LOGGING_INIT(ublk_drivers, libiscsi)

SISL_OPTIONS_ENABLE(logging)

namespace {

// Matches iscsi_tgtd_setup.sh defaults; can be overridden via env for parallel CI.
static std::string iscsi_url() {
    auto* port = std::getenv("ISCSI_TEST_PORT");
    auto* host = std::getenv("ISCSI_TEST_HOST");
    return std::string("iscsi://") + (host ? host : "127.0.0.1") + ":" + (port ? port : "13260") +
        "/iqn.2026-05.test.ublkpp:lun0/1";
}

// Probe the listener once per process. tgtd may not be installed (CI without
// the package, or contributor without tgt locally); when it isn't we skip
// rather than fail. The fixture script's "soft skip" exit-0 path means CTest
// would still run us; the runtime probe is what actually keeps things green.
static bool tgtd_reachable() {
    auto* port = std::getenv("ISCSI_TEST_PORT");
    auto* host = std::getenv("ISCSI_TEST_HOST");
    int p = port ? std::atoi(port) : 13260;
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast< uint16_t >(p));
    inet_pton(AF_INET, host ? host : "127.0.0.1", &addr.sin_addr);
    int rc = ::connect(sock, reinterpret_cast< sockaddr* >(&addr), sizeof(addr));
    ::close(sock);
    return 0 == rc;
}

// Aligned buffer helper (libiscsi tolerates unaligned, but we mirror FSDisk's
// O_DIRECT-style alignment so the same patterns transfer if/when we exercise
// async_iov through the real ublk path later).
class AlignedBuffer {
    void* _ptr{nullptr};
    size_t _size{0};

public:
    explicit AlignedBuffer(size_t size, size_t alignment = 512) : _size(size) {
        if (posix_memalign(&_ptr, alignment, size) != 0) _ptr = nullptr;
        if (_ptr) std::memset(_ptr, 0, size);
    }
    ~AlignedBuffer() {
        if (_ptr) std::free(_ptr);
    }
    AlignedBuffer(AlignedBuffer const&) = delete;
    AlignedBuffer& operator=(AlignedBuffer const&) = delete;
    void* get() { return _ptr; }
    uint8_t* data() { return static_cast< uint8_t* >(_ptr); }
    size_t size() const { return _size; }
};

class iSCSIDiskTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!tgtd_reachable()) GTEST_SKIP() << "tgtd not reachable on the test port; skipping iSCSIDisk tests";
    }
};

TEST_F(iSCSIDiskTest, ConstructorAndProbe) {
    std::unique_ptr< ublkpp::iSCSIDisk > disk;
    ASSERT_NO_THROW({ disk = std::make_unique< ublkpp::iSCSIDisk >(iscsi_url()); });
    EXPECT_GT(disk->capacity(), 0u);
    EXPECT_GT(disk->block_size(), 0u);
    // id() returns the target IQN (no LUN / portal suffix per current impl).
    EXPECT_NE(disk->id().find("iqn.2026-05.test.ublkpp:lun0"), std::string::npos);
}

TEST_F(iSCSIDiskTest, SyncReadWriteRoundtrip) {
    auto disk = std::make_unique< ublkpp::iSCSIDisk >(iscsi_url());
    auto const block_size = disk->block_size();

    AlignedBuffer write_buf(block_size);
    for (size_t i = 0; i < block_size; ++i)
        write_buf.data()[i] = static_cast< uint8_t >(i & 0xFF);

    iovec iov{.iov_base = write_buf.get(), .iov_len = block_size};
    auto wres = disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0);
    ASSERT_TRUE(wres.has_value()) << "write failed";
    EXPECT_EQ(wres.value(), block_size);

    AlignedBuffer read_buf(block_size);
    iov.iov_base = read_buf.get();
    auto rres = disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, 0);
    ASSERT_TRUE(rres.has_value()) << "read failed";
    EXPECT_EQ(rres.value(), block_size);
    EXPECT_EQ(0, std::memcmp(write_buf.get(), read_buf.get(), block_size));
}

TEST_F(iSCSIDiskTest, SyncVectoredReadWrite) {
    auto disk = std::make_unique< ublkpp::iSCSIDisk >(iscsi_url());
    auto const block_size = disk->block_size();

    AlignedBuffer w1(block_size), w2(block_size);
    std::memset(w1.get(), 0x11, block_size);
    std::memset(w2.get(), 0x22, block_size);

    iovec wivs[2] = {
        {.iov_base = w1.get(), .iov_len = block_size},
        {.iov_base = w2.get(), .iov_len = block_size},
    };
    auto wres = disk->sync_iov(UBLK_IO_OP_WRITE, wivs, 2, 0);
    ASSERT_TRUE(wres.has_value());
    EXPECT_EQ(wres.value(), 2 * block_size);

    AlignedBuffer r1(block_size), r2(block_size);
    iovec rivs[2] = {
        {.iov_base = r1.get(), .iov_len = block_size},
        {.iov_base = r2.get(), .iov_len = block_size},
    };
    auto rres = disk->sync_iov(UBLK_IO_OP_READ, rivs, 2, 0);
    ASSERT_TRUE(rres.has_value());
    EXPECT_EQ(rres.value(), 2 * block_size);
    EXPECT_EQ(0, std::memcmp(w1.get(), r1.get(), block_size));
    EXPECT_EQ(0, std::memcmp(w2.get(), r2.get(), block_size));
}

TEST_F(iSCSIDiskTest, SyncReadAtOffset) {
    auto disk = std::make_unique< ublkpp::iSCSIDisk >(iscsi_url());
    auto const block_size = disk->block_size();
    off_t const off = static_cast< off_t >(block_size) * 8;

    AlignedBuffer wbuf(block_size);
    std::memset(wbuf.get(), 0xCD, block_size);
    iovec iov{.iov_base = wbuf.get(), .iov_len = block_size};
    auto wres = disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, off);
    ASSERT_TRUE(wres.has_value());

    AlignedBuffer rbuf(block_size);
    iov.iov_base = rbuf.get();
    auto rres = disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, off);
    ASSERT_TRUE(rres.has_value());
    EXPECT_EQ(0, std::memcmp(wbuf.get(), rbuf.get(), block_size));
}

} // namespace

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    return RUN_ALL_TESTS();
}
