#include "ublkpp/ublkpp.hpp"

#include <coroutine>
#include <cstring>
#include <thread>

#include <boost/uuid/uuid_io.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/metrics/metrics.hpp>
#include <sisl/utility/thread_factory.hpp>
#include <ublksrv_utils.h>
#include <ublksrv.h>

#include "ublkpp/lib/ublk_disk.hpp"
#include "lib/logging.hpp"

SISL_OPTION_GROUP(ublkpp_tgt,
                  (max_io_size, "", "max_io_size", "Maximum I/O size before split",
                   cxxopts::value< std::uint32_t >()->default_value("524288"), "<io_size>"),
                  (nr_hw_queues, "", "nr_hw_queues", "Number of Hardware Queues (threads) per target",
                   cxxopts::value< std::uint16_t >()->default_value("1"), "<queue_cnt>"),
                  (qdepth, "", "qdepth", "I/O Queue Depth per target",
                   cxxopts::value< std::uint16_t >()->default_value("128"), "<qd>"))

using namespace std::chrono_literals;

namespace ublkpp {

struct UblkDiskMetrics : public sisl::MetricsGroupWrapper {
    explicit UblkDiskMetrics(std::string const& device_name) : sisl::MetricsGroup("UblkDisk", device_name.c_str()) {
        REGISTER_HISTOGRAM(ublk_write_queue_distribution, "Distribution of volume write queue depths",
                           HistogramBucketsType(SteppedUpto32Buckets));
        REGISTER_HISTOGRAM(ublk_read_queue_distribution, "Distribution of volume read queue depths",
                           HistogramBucketsType(SteppedUpto32Buckets));
        REGISTER_HISTOGRAM(device0_latency_us, "Device 0 I/O latency in microseconds",
                           HistogramBucketsType(ExponentialOfTwoBuckets));
        REGISTER_HISTOGRAM(device1_latency_us, "Device 1 I/O latency in microseconds",
                           HistogramBucketsType(ExponentialOfTwoBuckets));
        REGISTER_HISTOGRAM(device0_degraded_count, "Device 0 degradation events",
                           HistogramBucketsType(ExponentialOfTwoBuckets));
        REGISTER_HISTOGRAM(device1_degraded_count, "Device 1 degradation events",
                           HistogramBucketsType(ExponentialOfTwoBuckets));

        register_me_to_farm();
    }

    ~UblkDiskMetrics() { deregister_me_from_farm(); }
};

struct io_timing {
    std::chrono::steady_clock::time_point start_time;
    uint8_t device_id;
};

// Thread-local timing map: each queue handler thread tracks its own I/Os
// Key: (tag, sub_cmd) uniquely identifies an I/O within this thread
thread_local std::map<std::pair<uint16_t, uint16_t>, io_timing> t_io_timings;

struct ublkpp_tgt_impl {
    bool device_added{false};
    boost::uuids::uuid volume_uuid;
    std::filesystem::path device_path;
    // Owned by us
    std::shared_ptr< UblkDisk > device;
    std::unique_ptr< ublksrv_tgt_type const > tgt_type;

    // Owned by libublksrv
    ublksrv_ctrl_dev* ctrl_dev{nullptr};
    ublksrv_dev const* ublk_dev{nullptr};

    // == Metrics ==
    UblkDiskMetrics metrics;

    std::atomic_uint32_t _queued_reads;
    std::atomic_uint32_t _queued_writes;
    // == ======= ==

    // Owned by us
    std::unique_ptr< ublksrv_dev_data > dev_data;

    ublkpp_tgt_impl(boost::uuids::uuid const& vol_id, std::shared_ptr< UblkDisk > d) :
            volume_uuid(vol_id), device(std::move(d)), metrics(UblkDiskMetrics(to_string(vol_id))) {}

    ~ublkpp_tgt_impl();
};

static std::mutex _map_lock;
static std::map< ublksrv_ctrl_dev const*, std::shared_ptr< ublkpp_tgt_impl > > _init_map;

constexpr auto k_max_time = 1s;

static void check_dev(ublksrv_ctrl_dev_info const* info) {
    static auto const sys_path = std::filesystem::path{"/"} / "dev";
    auto const str_path = (sys_path / fmt::format("ublkc{}", info->dev_id)).native();

    auto wait = 0ms;
    while (wait < k_max_time) {
        if (int fd = open(str_path.c_str(), O_RDWR); fd > 0) {
            close(fd);
            break;
        }
        std::this_thread::sleep_for(100ms);
        wait += 100ms;
    }
}

static void set_queue_thread_affinity(ublksrv_ctrl_dev const*) {
    cpu_set_t set;

    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == -1) {
        TLOGE("sched_getaffinity, {}", strerror(errno))
        return;
    }

    srand(ublksrv_gettid());
    auto idx = rand() % CPU_COUNT(&set);

    int32_t j = 0;
    for (auto i = 0; i < CPU_SETSIZE; ++i) {
        if (CPU_ISSET(i, &set)) {
            if (j++ == idx) continue;
            CPU_CLR(i, &set);
        }
    }

    sched_setaffinity(0, sizeof(set), &set);
}

static void* ublksrv_queue_handler(std::shared_ptr< ublkpp_tgt_impl > target, int q_id, sem_t* queue_sem) {
    auto cdev = ublksrv_get_ctrl_dev(target->ublk_dev);

    ublk_json_write_queue_info(cdev, q_id, ublksrv_gettid());

    ublksrv_queue const* q;
    auto dev_id = ublksrv_ctrl_get_dev_info(cdev)->dev_id;
    if (q = ublksrv_queue_init_flags(target->ublk_dev, q_id, target.get(),
                                     IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SINGLE_ISSUER |
                                         IORING_SETUP_DEFER_TASKRUN);
        !q) {
        ublk_err("ublk dev %d queue %d init queue failed", dev_id, q_id);
        sem_post(queue_sem);
        target.reset();
        return NULL;
    }

    // select one cpu
    set_queue_thread_affinity(cdev);
    sem_post(queue_sem);

    target.reset();

    TLOGD("tid {}: ublk dev {} queue {} started", ublksrv_gettid(), dev_id, q->q_id)
    do {
        if (ublksrv_process_io(q) < 0) break;
    } while (1);

    TLOGD("ublk dev {} queue {} exited", dev_id, q->q_id)
    ublksrv_queue_deinit(q);
    return NULL;
}

static std::expected< std::filesystem::path, std::error_condition > start(std::shared_ptr< ublkpp_tgt_impl > tgt) {
    TLOGD("Initializing Ctrl Device")
    if (tgt->ctrl_dev = ublksrv_ctrl_init(tgt->dev_data.get()); !tgt->ctrl_dev) {
        TLOGE("Cannot init disk {}", tgt->device)
        return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
    }

    if (auto ret = ublksrv_ctrl_add_dev(tgt->ctrl_dev); 0 > ret) {
        TLOGE("Cannot add disk {}: {}", tgt->device, ret)
        return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
    }
    tgt->device_added = true;
    {
        auto info = ublksrv_ctrl_get_dev_info(tgt->ctrl_dev);
        tgt->dev_data->dev_id = info->dev_id;
    }
    // Let go of our shared_ptr to the target
    auto ctrl_dev = tgt->ctrl_dev;
    auto dev_ptr = tgt->device.get();

    auto const dinfo = ublksrv_ctrl_get_dev_info(ctrl_dev);
    auto const dev_id = dinfo->dev_id;

    // Wait for Ctrl device to appear
    check_dev(dinfo);

    if (auto ret = ublksrv_ctrl_get_affinity(ctrl_dev); 0 > ret) {
        TLOGE("dev {} get affinity failed {}", dev_id, ret)
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }

    TLOGD("Start ublksrv io daemon {}-{}", "ublkpp", dev_id)

    // Target is about to initialize! Insert into our map
    {
        auto lk = std::scoped_lock< std::mutex >(_map_lock);
        _init_map.emplace(std::make_pair(ctrl_dev, tgt));
    }

    tgt->ublk_dev = ublksrv_dev_init(ctrl_dev);

    {
        auto lk = std::scoped_lock< std::mutex >(_map_lock);
        _init_map.erase(ctrl_dev);
    }
    if (!tgt->ublk_dev) {
        TLOGE("dev-{} start ubsrv failed", dev_id)
        return std::unexpected(std::make_error_condition(std::errc::no_such_device));
    }

    // Unprivileged device support
    if (!(dinfo->flags & UBLK_F_UNPRIVILEGED_DEV)) ublksrv_apply_oom_protection();

    // Setup Queues
    sem_t queue_sem;
    sem_init(&queue_sem, 0, 0);
    for (auto i = 0; i < dinfo->nr_hw_queues; ++i) {
        sisl::named_thread(fmt::format("q_{}_{}", dev_id, i), ublksrv_queue_handler, tgt, i, &queue_sem).detach();
    }
    auto const dev_name = fmt::format("{}", *tgt->device);
    tgt.reset();

    // Wait for Queues to start
    for (auto i = 0; i < dinfo->nr_hw_queues; ++i)
        sem_wait(&queue_sem);

    // Start processing I/Os
    if (auto err = ublksrv_ctrl_set_params(ctrl_dev, dev_ptr->params()); err)
        return std::unexpected(std::error_condition(err, std::system_category()));
    if (auto err = ublksrv_ctrl_start_dev(ctrl_dev, getpid()); 0 > err)
        return std::unexpected(std::error_condition(err, std::system_category()));

    static auto const sys_path = std::filesystem::path{"/"} / "dev";
    auto const res = sys_path / fmt::format("ublkb{}", dev_id);
    TLOGI("{} exposed as UBD device: [{}]", dev_name, res.native());
    return res;
}

using co_handle_type = std::coroutine_handle<>;
struct co_io_job {
    struct promise_type {
        co_io_job get_return_object() { return {std::coroutine_handle< promise_type >::from_promise(*this)}; }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        [[noreturn]] void unhandled_exception() const { throw std::current_exception(); }
    };

    co_handle_type coro;

    co_io_job(co_handle_type h) : coro(h) {}

    operator co_handle_type() const { return coro; }
};

struct async_io {
    co_handle_type co;
    uint32_t sub_cmds;
    int ret_val;
    io_uring_cqe const* tgt_io_cqe;
    async_result const* async_completion;
};

// Process the result codes from the CQE or Async Completiong
static inline int retrieve_result(sub_cmd_t cmd, async_io* io) {
    int res{-EIO};
    if (auto cqe = io->tgt_io_cqe; cqe) {
        res = cqe->res;
    } else {
        DEBUG_ASSERT_NOTNULL(io->async_completion, "No completion to process!");
        auto a_result = *io->async_completion;
        res = a_result.result;
    }
    // Only return Errors from DEPENDENT or REPLICATE commands
    if ((0 < res) && (is_dependent(cmd) || is_replicate(cmd))) return 0;
    return res;
}

static void process_result(ublksrv_queue const* q, ublk_io_data const* data) {
    auto device = reinterpret_cast< UblkDisk* >(q->dev->tgt.tgt_data);
    auto ublkpp_io = reinterpret_cast< async_io* >(data->private_data);
    --ublkpp_io->sub_cmds;
    sub_cmd_t const old_cmd = (ublkpp_io->tgt_io_cqe ? user_data_to_tgt_data(ublkpp_io->tgt_io_cqe->user_data)
                                                     : ublkpp_io->async_completion->sub_cmd);

    // Record I/O completion for device latency tracking
    UblkDisk::record_io_complete(q, data, old_cmd);

    // If >= 0, the sub_cmd succeeded, aggregate the repsonses from each sum_cmd into the final io result.
    auto sub_cmd_res = retrieve_result(old_cmd, ublkpp_io);

    // Internal commands are not part of the result like a replicate command, but we do inform the devices of the result
    if (is_internal(old_cmd)) {
        auto io_res = device->queue_internal_resp(q, data, old_cmd, sub_cmd_res);
        DEBUG_ASSERT(io_res, "HandleInternal return error!") // LCOV_EXCL_LINE
        // Could enqueue more requeusts
        if (io_res) ublkpp_io->sub_cmds += io_res.value();
        sub_cmd_res = 0;
    }

    // Error should be returned regardless of other responses
    if (0 > ublkpp_io->ret_val) {
        TLOGT("I/O result ignored [tag:{:0x}|sub_cmd:{}] [sub_cmds_remain:{}]", data->tag, to_string(old_cmd),
              ublkpp_io->sub_cmds)
        return;
    }
    TLOGT("I/O result: [{}] [tag:{:0x}|sub_cmd:{}] [sub_cmds_remain:{}]", sub_cmd_res, data->tag, to_string(old_cmd),
          ublkpp_io->sub_cmds)

    if (0 <= sub_cmd_res) {
        ublkpp_io->ret_val += sub_cmd_res;
        return;
    }

    // Do not retry a already Retried or Dependent commands
    if (is_retry(old_cmd) || is_dependent(old_cmd)) {
        ublkpp_io->ret_val = sub_cmd_res;
        return;
    }

    // If retriable, pass the original sub_cmd the sub_cmd took in addition to re-queuing the original
    // operation. This provides the context to the RAID layers to make intelligent decisions for a retried
    // sub_cmd.
    auto const sub_cmd = set_flags(old_cmd, sub_cmd_flags::RETRIED);
    TLOGD("Retrying portion of I/O [res:{}] [tag:{:0x}] [sub_cmd:{}]", sub_cmd_res, data->tag, to_string(sub_cmd))
    auto io_res = device->queue_tgt_io(q, data, sub_cmd);

    // Submit to io_uring before yielding to make iovecs that are thread_local stable
    io_uring_submit(q->ring_ptr);

    if (!io_res) {
        TLOGE("Retry Failed Immediately on I/O [tag:{:0x}] [sub_cmd:{}] [err:{}]", data->tag, to_string(sub_cmd),
              io_res.error().message())
        ublkpp_io->ret_val = sub_cmd_res;
        return;
    }
    // New sub_cmds to wait for in the co-routine
    ublkpp_io->sub_cmds += io_res.value();
}

static co_io_job __handle_io_async(ublksrv_queue const* q, ublk_io_data const* data) {
    auto device = reinterpret_cast< UblkDisk* >(q->dev->tgt.tgt_data);
    auto ublkpp_io = reinterpret_cast< async_io* >(data->private_data);
    ublkpp_io->ret_val = -EIO;
    ublkpp_io->sub_cmds = 0;
    ublkpp_io->tgt_io_cqe = nullptr;
    ublkpp_io->async_completion = nullptr;

    auto const op = ublksrv_get_op(data->iod);
    auto tgt = static_cast< ublkpp_tgt_impl* >(q->private_data);
    switch (op) {
    case UBLK_IO_OP_READ: {
        HISTOGRAM_OBSERVE(tgt->metrics, ublk_read_queue_distribution,
                          tgt->_queued_reads.fetch_add(1, std::memory_order_relaxed) + 1);
    } break;
    case UBLK_IO_OP_WRITE: {
        HISTOGRAM_OBSERVE(tgt->metrics, ublk_write_queue_distribution,
                          tgt->_queued_writes.fetch_add(1, std::memory_order_relaxed) + 1);
    } break;
    default: {
    }
    }

    // First we submit the IO to the UblkDisk device. It in turn will return the number
    // of sub_cmd's it enqueued to the io_uring queue to satisfy the request. RAID levels will
    // cause this amplification of operations.
    auto io_res = device->queue_tgt_io(q, data, 0);

    // Submit to io_uring before yielding to make iovecs that are thread_local stable
    io_uring_submit(q->ring_ptr);

    if (io_res) {
        ublkpp_io->ret_val = 0;
        ublkpp_io->sub_cmds = io_res.value();
        TLOGT("I/O [tag:{:0x}] [sub_ios:{}]", data->tag, ublkpp_io->sub_cmds)
    } else
        TLOGD("IO Failed Immediately to queue io [tag:{:0x}], err: [{}]", data->tag, io_res.error().message())

    // For each sub_cmd enqueued, we expect a response to be processed.
    while (0 < ublkpp_io->sub_cmds) {
        { co_await std::suspend_always(); }
        process_result(q, data);
    }

    switch (op) {
    case UBLK_IO_OP_READ: {
        tgt->_queued_reads.fetch_sub(1, std::memory_order_relaxed);
    } break;
    case UBLK_IO_OP_WRITE: {
        tgt->_queued_writes.fetch_sub(1, std::memory_order_relaxed);
    } break;
    default: {
    }
    }

    // Operation is complete, result is in io_res
    if (0 > ublkpp_io->ret_val) [[unlikely]] {
        TLOGE("Returning error for [tag:{:0x}] [res:{}]", data->tag, ublkpp_io->ret_val)
    } else {
        TLOGT("I/O complete [tag:{:0x}] [res:{}]", data->tag, ublkpp_io->ret_val)
    }
    ublksrv_complete_io(q, data->tag, ublkpp_io->ret_val);
}

// I/O Handler, first entry-point to us for all I/O
static int handle_io_async(ublksrv_queue const* q, ublk_io_data const* data) {
    // Construct a co-routine and set it to the private data, we call resume in `tgt_io_done` once complete
    reinterpret_cast< async_io* >(data->private_data)->co = __handle_io_async(q, data);
    return 0;
}

// Called when the I/O we have scheduled on the ublksrv uring (e.g. FSDisk) have completed
static void tgt_io_done(ublksrv_queue const* q, ublk_io_data const* data, io_uring_cqe const* cqe) {
    auto tag = static_cast< int32_t >(user_data_to_tag(cqe->user_data));
    auto io = reinterpret_cast< async_io* >(data->private_data);

    RELEASE_ASSERT_EQ(data->tag, tag, "Tag mismatch!")
    io->tgt_io_cqe = cqe;
    try {
        io->co.resume();
    } catch (std::exception const& e) {
        TLOGE("I/O threw exception: [{}]", e.what())
        ublksrv_complete_io(q, data->tag, -EIO);
    }
}

// Called when some async UblkDisk has called send_event to notify of a sub_cmd completion
static void handle_event(ublksrv_queue const* q) {
    auto tgt = static_cast< ublkpp_tgt_impl* >(q->private_data);
    auto completed = std::list< async_result >();
    tgt->device->collect_async(q, completed);

    // Clear the event from efd first, as we may cause new events by retrying failed sub_cmds
    ublksrv_queue_handled_event(q);
    for (auto& result : completed) {
        try {
            auto ublkpp_io = reinterpret_cast< async_io* >(result.io->private_data);
            // We set this to indicate to the co-routine this is an async result
            // not something to parse from io_uring
            ublkpp_io->tgt_io_cqe = nullptr;
            ublkpp_io->async_completion = &result;
            ublkpp_io->co.resume();
        } catch (std::exception const& e) { ublksrv_complete_io(q, result.io->tag, -EIO); }
    }
}

static void idle_transition(ublksrv_queue const* q, bool enter) {
    auto tgt = static_cast< ublkpp_tgt_impl* >(q->private_data);
    TLOGT("Idle Trans: {}", enter)
    tgt->device->idle_transition(q, enter);
}

// Called in the context of start by ublksrv_dev_init()
static int init_tgt(ublksrv_dev* dev, int, int, char*[]) {
    // Find the registered disk in the disk map and set the tgt_data
    // to its RAW pointer. This will be handed to handle_io_async for each I/O
    // so we have the context of which disk is receiving I/O.
    auto tgt = std::shared_ptr< ublkpp_tgt_impl >();
    auto cdev = ublksrv_get_ctrl_dev(dev);
    {
        auto lk = std::scoped_lock< std::mutex >(_map_lock);
        if (auto it = _init_map.find(cdev); _init_map.end() != it) { tgt = it->second; }
    }
    if (!tgt) {
        TLOGE("Disk not found in map!")
        return -2;
    }
    auto ublk_disk = tgt->device;
    dev->tgt.tgt_data = ublk_disk.get();

    // Configure the ublksrv JSON bits
    if (!ublksrv_is_recovering(cdev)) {
        auto tgt_json = ublksrv_tgt_base_json{
            .name = "",
            .type = 0,
            .pad = 0,
            .dev_size = 0,
            .reserved = {0},
        };

        auto str_id = to_string(tgt->volume_uuid);
        std::erase(str_id, '-');
        RELEASE_ASSERT_EQ(str_id.size(), UBLKSRV_TGT_NAME_MAX_LEN, "Bad UUID length!")
        strncpy(tgt_json.name, str_id.c_str(), UBLKSRV_TGT_NAME_MAX_LEN - 1);
        tgt_json.dev_size = ublk_disk->params()->basic.dev_sectors << SECTOR_SHIFT;
        ublk_json_write_dev_info(cdev);
        ublk_json_write_target_base(cdev, &tgt_json);
        ublk_json_write_params(cdev, ublk_disk->params());
    }

    auto ublksrv_tgt = &dev->tgt;
    ublksrv_tgt->io_data_size = sizeof(struct async_io);
    ublksrv_tgt->dev_size = ublk_disk->params()->basic.dev_sectors << SECTOR_SHIFT;
    ublksrv_tgt->tgt_ring_depth = ublksrv_ctrl_get_dev_info(ublksrv_get_ctrl_dev(dev))->queue_depth;

    // iouring FD 0 is reserved for the ublkc device, so start gathering from there
    ublksrv_tgt->nr_fds = 1;
    for (auto const fd : ublk_disk->open_for_uring(ublksrv_tgt->nr_fds)) {
        ublksrv_tgt->fds[ublksrv_tgt->nr_fds++] = fd;
    }
    return 0;
}

// Setup ublksrv ctrl device and initiate adding the target to the ublksrv service and handle all device traffic
ublkpp_tgt::run_result_t ublkpp_tgt::run(boost::uuids::uuid const& vol_id, std::shared_ptr< UblkDisk > device) {
    auto tgt = std::make_shared< ublkpp_tgt_impl >(vol_id, device);
    tgt->tgt_type = std::make_unique< ublksrv_tgt_type >(ublksrv_tgt_type{
        .handle_io_async = handle_io_async,
        .tgt_io_done = tgt_io_done,
        .handle_event = device->uses_ublk_iouring
            ? nullptr
            : handle_event, // Device specific, determines *if* ublksrv_complete_io() will be called by device
        .handle_io_background = nullptr, // Not Implemented
        .usage_for_add = nullptr,        // Not Implemented
        .init_tgt = init_tgt,
        .deinit_tgt = nullptr,      // Not Implemented
        .alloc_io_buf = nullptr,    // Not Implemented
        .free_io_buf = nullptr,     // Not Implemented
        .idle_fn = idle_transition, // Called when I/O has stopped
        .type = 0,                  // Deprecated
        .ublk_flags = 0,            // Currently Clear
        .ublksrv_flags = (device->uses_ublk_iouring ? 0U : (unsigned)UBLKSRV_F_NEED_EVENTFD), // See handle_event
        .pad = 0,                                                                             // Currently Clear
        .name = "ublkpp",
        .recovery_tgt = nullptr,    // Deprecated
        .init_queue = nullptr,      // Not Implemented
        .deinit_queue = nullptr,    // Not Implemented
        .reserved = {0, 0, 0, 0, 0} // Reserved
    });

    TLOGD("Starting {} {} evfd", static_pointer_cast< UblkDisk >(device),
          (nullptr == tgt->tgt_type->handle_event) ? "WITHOUT" : "WITH")
    tgt->dev_data = std::make_unique< ublksrv_dev_data >(ublksrv_dev_data{
        .dev_id = -1,
        .max_io_buf_bytes = SISL_OPTIONS["max_io_size"].as< uint32_t >(),
        .nr_hw_queues = SISL_OPTIONS["nr_hw_queues"].as< uint16_t >(),
        .queue_depth = SISL_OPTIONS["qdepth"].as< uint16_t >(),
        .tgt_type = "ublkpp",
        .tgt_ops = tgt->tgt_type.get(),
        .tgt_argc = 0,
        .tgt_argv = nullptr,
        .run_dir = nullptr,
        .flags = tgt->tgt_type->ublk_flags,
        .ublksrv_flags = tgt->tgt_type->ublksrv_flags,
        .reserved = {0, 0, 0, 0, 0, 0, 0},
    });
    auto res = start(tgt);
    if (!res) return std::unexpected(res.error());
    tgt->device_path = res.value();

    auto new_tgt = new ublkpp_tgt(tgt);
    return std::unique_ptr< ublkpp_tgt >(new_tgt);
}

ublkpp_tgt::ublkpp_tgt(std::shared_ptr< ublkpp_tgt_impl > p) : _p(p) {}

ublkpp_tgt::~ublkpp_tgt() = default;

std::filesystem::path ublkpp_tgt::device_path() const { return _p->device_path; }
std::shared_ptr< UblkDisk > ublkpp_tgt::device() const { return _p->device; }

ublkpp_tgt_impl::~ublkpp_tgt_impl() {
    TLOGD("Stopping {}", device)
    if (ublk_dev) {
        ublksrv_ctrl_stop_dev(ctrl_dev);
        ublksrv_dev_deinit(ublk_dev);
    }
    if (device_added) ublksrv_ctrl_del_dev(ctrl_dev);
    if (ctrl_dev) ublksrv_ctrl_deinit(ctrl_dev);
    TLOGD("Stopped {}", device)
}

void UblkDisk::record_io_start(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint8_t device_id) {
    if (!q || !q->private_data || !data) return;

    auto key = std::make_pair(static_cast<uint16_t>(data->tag), static_cast<uint16_t>(sub_cmd));
    t_io_timings[key] = io_timing{std::chrono::steady_clock::now(), device_id};
}

void UblkDisk::record_io_complete(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) {
    if (!q || !q->private_data || !data) return;

    auto tgt = static_cast<ublkpp_tgt_impl*>(q->private_data);
    auto key = std::make_pair(static_cast<uint16_t>(data->tag), static_cast<uint16_t>(sub_cmd));

    if (auto it = t_io_timings.find(key); it != t_io_timings.end()) {
        auto const& timing = it->second;
        auto const end_time = std::chrono::steady_clock::now();
        auto const latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - timing.start_time).count();

        // Record to appropriate histogram
        if (timing.device_id == 0) {
            HISTOGRAM_OBSERVE(tgt->metrics, device0_latency_us, latency_us);
        } else if (timing.device_id == 1) {
            HISTOGRAM_OBSERVE(tgt->metrics, device1_latency_us, latency_us);
        }

        t_io_timings.erase(it);
    }
}

void UblkDisk::record_device_degraded(ublksrv_queue const* q, uint8_t device_id) {
    if (!q || !q->private_data) return;

    auto tgt = static_cast<ublkpp_tgt_impl*>(q->private_data);

    if (device_id == 0) {
        HISTOGRAM_OBSERVE(tgt->metrics, device0_degraded_count, 1);
    } else if (device_id == 1) {
        HISTOGRAM_OBSERVE(tgt->metrics, device1_degraded_count, 1);
    }
}

} // namespace ublkpp
