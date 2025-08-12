#include <future>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublkpp/lib/ublk_disk.hpp>

SISL_OPTIONS_ENABLE(logging)
SISL_LOGGING_INIT(ublksrv)

namespace ublkpp {
class InMemoryDisk : public UblkDisk {
public:
    explicit InMemoryDisk(uint64_t capacity) {}
    ~InMemoryDisk() override {}

    std::string id() const override { return "InMemoryDisk"; }
    bool contains(std::string const&) const { return false; }
    std::list< int > open_for_uring(int const iouring_device) override { return {}; }

    void collect_async(ublksrv_queue const*, std::list< async_result >& compl_list) override { return; }
    io_result handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) override { return 0; }
    io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                             uint64_t addr) {
        LOGINFO("received DISCARD: [addr:{}|len:{}]", addr, len);
        return 0;
    }

    io_result async_iov(ublksrv_queue const* q, ublk_io_data const*, sub_cmd_t sub_cmd, iovec* iovecs, uint32_t nr_vecs,
                        uint64_t addr) override {
        LOGINFO("Received [addr:{}|len:{}] [sub_cmd:{}]", addr, __iovec_len(iovecs, iovecs + nr_vecs),
                ublkpp::to_string(sub_cmd));
        return 0;
    }

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept override {
        LOGINFO("Received [addr:{}|len:{}]", addr, __iovec_len(iovecs, iovecs + nr_vecs))
        return 0;
    }
};
} // namespace ublkpp

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]),
                             BOOST_PP_STRINGIZE(PACKAGE_NAME), BOOST_PP_STRINGIZE(PACKAGE_VERSION));
    spdlog::set_pattern("[%D %T] [%^%l%$] [%n] [%t] %v");
    auto in_memory_disk = std::make_shared< ublkpp::InMemoryDisk >(256 * ublkpp::Mi);
}
