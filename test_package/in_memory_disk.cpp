#include <sys/uio.h>

#include <future>
#include <numeric>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublkpp/lib/ublk_disk.hpp>

SISL_OPTIONS_ENABLE(logging)
SISL_LOGGING_INIT(ublksrv)

namespace ublkpp {
class InMemoryDisk : public ublk_disk {
public:
    explicit InMemoryDisk(uint64_t capacity) { params()->basic.dev_sectors = capacity >> 9; }
    ~InMemoryDisk() override {}

    std::string id() const noexcept override { return "InMemoryDisk"; }

    disk_task< int > async_iov(ublksrv_queue const*, ublk_io_data const*, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override {
        LOGINFO("Received [addr:{}|len:{}]", addr,
                std::accumulate(iovecs, iovecs + nr_vecs, 0UL, [](auto a, iovec const& v) { return a + v.iov_len; }));
        co_return 0;
    }

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept override {
        LOGINFO("Received [addr:{}|len:{}]", addr,
                std::accumulate(iovecs, iovecs + nr_vecs, 0UL, [](auto a, iovec const& v) { return a + v.iov_len; }))
        return 0;
    }
};
} // namespace ublkpp

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]),
                             BOOST_PP_STRINGIZE(PACKAGE_NAME), BOOST_PP_STRINGIZE(PACKAGE_VERSION));
    spdlog::set_pattern("[%D %T] [%^%l%$] [%n] [%t] %v");
    auto in_memory_disk = std::make_shared< ublkpp::InMemoryDisk >(256ULL << 20);
}
