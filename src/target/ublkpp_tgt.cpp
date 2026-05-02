#include "ublkpp/ublkpp.hpp"

#include <exec/async_scope.hpp>
#include <exec/inline_scheduler.hpp>
#include <exec/task.hpp>
#include <stdexec/execution.hpp>
#include <thread>

#include <boost/uuid/uuid_io.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/utility/thread_factory.hpp>
#include <ublksrv_utils.h>
#include <ublksrv.h>

#include "ublkpp/lib/ublk_disk.hpp"
#include "lib/logging.hpp"
#include <ublkpp/lib/cqe_state.hpp>
#include "ublkpp_tgt_impl.hpp"

SISL_OPTION_GROUP(ublkpp_tgt,
                  (max_io_size, "", "max_io_size", "Maximum I/O size before split",
                   cxxopts::value< std::uint32_t >()->default_value("524288"), "<io_size>"),
                  (nr_hw_queues, "", "nr_hw_queues", "Number of Hardware Queues (threads) per target",
                   cxxopts::value< std::uint16_t >()->default_value("1"), "<queue_cnt>"),
                  (qdepth, "", "qdepth", "I/O Queue Depth per target",
                   cxxopts::value< std::uint16_t >()->default_value("32"), "<qd>"),
                  (feature_zero_copy, "", "feature_zero_copy", "Enable ZeroCopy Feature", cxxopts::value< bool >(), ""))

using namespace std::chrono_literals;

namespace ublkpp {

ublkpp_tgt_impl::ublkpp_tgt_impl(boost::uuids::uuid const& vol_id, std::shared_ptr< UblkDisk > d) :
        volume_uuid(vol_id), device(std::move(d)), metrics(UblkIOMetrics(to_string(vol_id))) {}

static std::mutex _map_lock;
static std::map< ublksrv_ctrl_dev const*, std::shared_ptr< ublkpp_tgt_impl > > _init_map;

constexpr auto k_max_time = 1s;

static bool check_dev(ublksrv_ctrl_dev_info const* info) {
    static auto const sys_path = std::filesystem::path{"/"} / "dev";
    auto const str_path = (sys_path / fmt::format("ublkc{}", info->dev_id)).native();

    auto wait = 0ms;
    while (wait < k_max_time) {
        if (int fd = open(str_path.c_str(), O_RDWR); fd >= 0) {
            close(fd);
            return true;
        }
        std::this_thread::sleep_for(100ms);
        wait += 100ms;
    }
    return false;
}

// bit 63 = is_target_io (matches ublksrv_priv.h encoding)
static constexpr uint64_t k_target_bit = 1ULL << 63;
// Matches UBLKSRV_IO_IDLE_SECS defined privately in ublksrv.c
static constexpr int k_io_idle_secs = 20;

struct ublkpp_queue_state {
    ublkpp_tgt_impl* tgt;
    exec::async_scope scope;

    explicit ublkpp_queue_state(ublkpp_tgt_impl* t) : tgt(t) {}
};

// Our own CQE processing loop, replacing ublksrv_process_io.
// Target CQEs carry a raw CqeState* in bits 62:0 with bit 63 set; decoded via pointer cast.
// Ublk command CQEs delegate to ublksrv.
//
// Drain correctness: ublksrv_queue_is_done returns true only when ublksrv has no pending I/O
// commands. We call ublksrv_complete_io at the end of __handle_io_async, after co_await
// device->async_iov returns, so the scope is already empty when the loop exits. on_empty()
// completes synchronously via its fast path.
static exec::task< void > run_queue_loop(ublksrv_queue const* q, ublkpp_queue_state* qs) {
    auto* ring = q->ring_ptr;
    // clang-format off
    struct __kernel_timespec ts{.tv_sec = k_io_idle_secs, .tv_nsec = 0};
    // clang-format on
    bool queue_done = false;

    while (!queue_done) {
        io_uring_cqe* cqe{};
        auto const ret = io_uring_submit_and_wait_timeout(ring, &cqe, 1, &ts, nullptr);

        unsigned head{};
        int count{0};
        io_uring_for_each_cqe(ring, head, cqe) {
            if (cqe->user_data & k_target_bit) {
                // target io_uring CQE -- user_data is a raw CqeState* | k_target_bit
                auto* state = reinterpret_cast< CqeState* >(cqe->user_data & ~k_target_bit);
                state->result = cqe->res;
                state->result_ready = true;
                try {
                    if (auto h = std::exchange(state->waiter, {})) h.resume(); // per-state resume (disk_task path)
                } catch (std::exception const& e) {
                    TLOGE("I/O threw exception: [{}]", e.what())
                    ublksrv_complete_io(q, state->owner->tag, -EIO);
                }
            } else {
                // ublk command CQE (FETCH/COMMIT) -- delegate to libublksrv
                ublksrv_handle_cmd_cqe(q, cqe);
            }
            ++count;
        }
        io_uring_cq_advance(ring, count);
        ublksrv_queue_update_idle(q, ret, count);
        queue_done = ublksrv_queue_is_done(q);
    }

    co_await qs->scope.on_empty();
}

static void* ublksrv_queue_handler(std::shared_ptr< ublkpp_tgt_impl > target, int q_id, sem_t* queue_sem) {
    auto qs = std::make_unique< ublkpp_queue_state >(target.get());

    // Initialize UBlkSrv IOUring queue and bind queue state pointer
    // NOTE: Removed IORING_SETUP_DEFER_TASK as it was blocking ublksrv_ctrl_del_dev,
    // look at adding this back as it theoretically could improve performance.
    auto q = ublksrv_queue_init_flags(target->ublk_dev, q_id, qs.get(),
                                      IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SINGLE_ISSUER);

    // Wake up ::start() thread
    sem_post(queue_sem);
    target.reset();

    // If queue initialization failed, exit
    if (!q) {
        TLOGE("ublk dev queue {} init queue failed", q_id)
        return NULL;
    }

    TLOGD("tid {}: ublk dev queue {} started", ublksrv_gettid(), q->q_id)
    stdexec::sync_wait(run_queue_loop(q, qs.get()));
    TLOGD("ublk dev queue {} exited", q->q_id)
    ublksrv_queue_deinit(q);
    return NULL;
}

static std::expected< std::filesystem::path, std::error_condition > start(std::shared_ptr< ublkpp_tgt_impl > tgt) {
    TLOGD("Initializing Ctrl Device")
    if (!tgt->device_recovering) { // NORMAL Path
        if (tgt->ctrl_dev = ublksrv_ctrl_init(tgt->dev_data.get()); !tgt->ctrl_dev) {
            TLOGE("Cannot init disk {}", tgt->device)
            return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
        }
        if (auto ret = ublksrv_ctrl_add_dev(tgt->ctrl_dev); 0 > ret) {
            TLOGE("Cannot add disk {}: {}", tgt->device, ret)
            return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
        }
    } else { // RECOVERY Path
        if (tgt->ctrl_dev = ublksrv_ctrl_recover_init(tgt->dev_data.get()); !tgt->ctrl_dev) {
            TLOGE("Cannot recover disk {}", tgt->device)
            return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
        }
        if (auto ret = ublksrv_ctrl_get_info(tgt->ctrl_dev); ret < 0) {
            TLOGE("Cannot get Ctrl Info for disk {}", tgt->device)
            return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
        } else {
            ret = ublksrv_ctrl_start_recovery(tgt->ctrl_dev);
        }
    }

    auto const dinfo = ublksrv_ctrl_get_dev_info(tgt->ctrl_dev);

    tgt->device_added = true;
    tgt->dev_data->dev_id = dinfo->dev_id;

    // Wait for Ctrl device to appear
    if (!check_dev(dinfo)) {
        TLOGE("dev {} never saw control device", tgt->dev_data->dev_id)
        return std::unexpected(std::make_error_condition(std::errc::no_such_device));
    }

    if (auto ret = ublksrv_ctrl_get_affinity(tgt->ctrl_dev); 0 > ret) {
        TLOGE("dev {} get affinity failed {}", tgt->dev_data->dev_id, ret)
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }

    TLOGD("Start ublksrv io daemon {}-{}", "ublkpp", tgt->dev_data->dev_id)

    // Target is about to initialize! Insert into our map
    {
        auto lk = std::scoped_lock< std::mutex >(_map_lock);
        _init_map.emplace(std::make_pair(tgt->ctrl_dev, tgt));
    }

    tgt->ublk_dev = ublksrv_dev_init(tgt->ctrl_dev);

    {
        auto lk = std::scoped_lock< std::mutex >(_map_lock);
        _init_map.erase(tgt->ctrl_dev);
    }
    if (!tgt->ublk_dev) {
        TLOGE("dev-{} start ublksrv failed", tgt->dev_data->dev_id)
        return std::unexpected(std::make_error_condition(std::errc::no_such_device));
    }

    // Setup Queues
    sem_t queue_sem;
    sem_init(&queue_sem, 0, 0);
    for (auto i = 0; i < dinfo->nr_hw_queues; ++i) {
        tgt->queue_handlers.push_back(sisl::named_thread(fmt::format("q_{}_{}", tgt->dev_data->dev_id, i),
                                                         ublksrv_queue_handler, tgt, i, &queue_sem));
    }
    auto const recovery = tgt->device_recovering;
    auto const dev_name = fmt::format("{}", *tgt->device);

    // Let go of our shared_ptr to the target
    auto ctrl_dev = tgt->ctrl_dev;
    auto dev_ptr = tgt->device.get();
    auto const dev_id = tgt->dev_data->dev_id;
    tgt.reset();

    // Wait for Queues to start
    for (auto i = 0; i < dinfo->nr_hw_queues; ++i)
        sem_wait(&queue_sem);

    // Start processing I/Os
    if (!recovery) {
        if (auto err = ublksrv_ctrl_set_params(ctrl_dev, dev_ptr->params()); err)
            return std::unexpected(std::error_condition(err, std::system_category()));
        if (auto err = ublksrv_ctrl_start_dev(ctrl_dev, getpid()); 0 > err)
            return std::unexpected(std::error_condition(err, std::system_category()));
    } else if (auto err = ublksrv_ctrl_end_recovery(ctrl_dev, getpid()); 0 > err) {
        return std::unexpected(std::error_condition(err, std::system_category()));
    }

    static auto const sys_path = std::filesystem::path{"/"} / "dev";
    auto const res = sys_path / fmt::format("ublkb{}", dev_id);
    TLOGI("{} exposed as UBD device: [{}]", dev_name, res.native());
    return res;
}

static exec::task< void > __handle_io_async(ublksrv_queue const* q, ublk_io_data const* data) {
    auto* qs = static_cast< ublkpp_queue_state* >(q->private_data);

    auto device = reinterpret_cast< UblkDisk* >(q->dev->tgt.tgt_data);
    auto io = reinterpret_cast< async_io* >(data->private_data);
    io->pool.clear();
    io->tag = data->tag;

    auto const op = ublksrv_get_op(data->iod);
    qs->tgt->metrics.record_queue_depth_change(q, op, true);

    int result;
    if (op == UBLK_IO_OP_FLUSH) {
        result = 0;
    } else {
        auto const* iod = data->iod;
        // Frame-local: io_uring reads iov contents at submit time (deferred to the queue loop's
        // submit_and_wait_timeout). thread_local would be overwritten by sibling __handle_io_async
        // coroutines spawned in the same CQE batch before the kernel sees the SQE. The coroutine
        // frame is alive across co_await, so the iov is valid through the whole IO lifetime.
        iovec iov{.iov_base = reinterpret_cast< void* >(iod->addr), .iov_len = iod->nr_sectors << SECTOR_SHIFT};
        result = co_await device->async_iov(q, data, &iov, 1, iod->start_sector << SECTOR_SHIFT);
    }

    qs->tgt->metrics.record_queue_depth_change(q, op, false);

    if (0 > result) [[unlikely]] {
        TLOGE("Returning error for [tag:{:#0x}] [res:{}]", data->tag, result)
    } else {
        TLOGT("I/O complete [tag:{:#0x}] [res:{}]", data->tag, result)
    }
    ublksrv_complete_io(q, data->tag, result);
}

// I/O Handler, first entry-point to us for all I/O
static int handle_io_async(ublksrv_queue const* q, ublk_io_data const* data) {
    auto* qs = static_cast< ublkpp_queue_state* >(q->private_data);
    qs->scope.spawn(stdexec::on(exec::inline_scheduler{}, __handle_io_async(q, data)));
    return 0;
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

    // TODO Ublk Recovery
    // if (ublksrv_is_recovering(cdev)) {
    //}

    auto ublksrv_tgt = &dev->tgt;
    ublksrv_tgt->io_data_size = sizeof(struct async_io);
    ublksrv_tgt->dev_size = ublk_disk->params()->basic.dev_sectors << SECTOR_SHIFT;
    ublksrv_tgt->tgt_ring_depth = 256;

    // iouring FD 0 is reserved for the ublkc device; prepare is called per queue in init_queue.
    // NOTE: if future disks export non-empty FDs they must be registered here (before ublksrv_queue_init
    // calls io_uring_register_files). For now all disks return empty so init_queue suffices.
    ublksrv_tgt->nr_fds = 1;
    return 0;
}

static void deinit_tgt(const struct ublksrv_dev*) { TLOGD("Deinit tgt!") }
static void idle_transition(ublksrv_queue const* q, bool enter) {
    TLOGT("Idle Trans: {}", enter)
    auto device = reinterpret_cast< UblkDisk* >(q->dev->tgt.tgt_data);
    device->idle_transition(q, enter);
}
static int init_queue(const struct ublksrv_queue* q, void**) {
    TLOGD("Init Queue")
    auto device = reinterpret_cast< UblkDisk* >(q->dev->tgt.tgt_data);
    // All current disk types return no FDs from per-queue init; non-empty means the FDs would go
    // unregistered with io_uring and fixed-file I/O would crash — treat it as a fatal init failure.
    if (!device->prepare(q, 0).empty()) return -1;
    // async_io contains std::deque members; ublksrv allocates raw bytes via calloc, so we must
    // placement-new to properly construct each IO slot's async_io.
    for (int i = 0; i < q->q_depth; ++i)
        new (ublksrv_io_private_data(q, i)) async_io{};
    return 0;
}
static void deinit_queue(const struct ublksrv_queue* q) {
    TLOGD("Deinit Queue")
    for (int i = 0; i < q->q_depth; ++i)
        static_cast< async_io* >(ublksrv_io_private_data(q, i))->~async_io();
}

// Setup ublksrv ctrl device and initiate adding the target to the ublksrv service and handle all device traffic
ublkpp_tgt::run_result_t ublkpp_tgt::run(boost::uuids::uuid const& vol_id, std::shared_ptr< UblkDisk > device,
                                         int device_id) {
    auto tgt = std::make_shared< ublkpp_tgt_impl >(vol_id, device);
    if (0 <= device_id) tgt->device_recovering = true;
    auto ublk_flags = unsigned(0);
    ublk_flags |= (unsigned)(UBLK_F_USER_RECOVERY | UBLK_F_USER_RECOVERY_REISSUE);
    if (0 < SISL_OPTIONS["feature_zero_copy"].count()) {
        TLOGI("Enabling zero-copy support...: {}", to_string(vol_id))
        ublk_flags |= (unsigned)(UBLK_F_SUPPORT_ZERO_COPY);
    }

    tgt->tgt_type = std::make_unique< ublksrv_tgt_type >(ublksrv_tgt_type{
        .handle_io_async = handle_io_async,
        .tgt_io_done = nullptr,  // handled inline in run_queue_loop
        .handle_event = nullptr, // handled inline in run_queue_loop
        .handle_io_background = nullptr,
        .usage_for_add = nullptr, // Not Implemented
        .init_tgt = init_tgt,
        .deinit_tgt = deinit_tgt,
        .alloc_io_buf = nullptr,    // Not Implemented
        .free_io_buf = nullptr,     // Not Implemented
        .idle_fn = idle_transition, // Called when I/O has stopped
        .type = 0,                  // Deprecated *DO NOT USE*
        .ublk_flags = ublk_flags,
        .ublksrv_flags = 0,
        .pad = 0, // Currently Clear
        .name = "ublkpp",
        .recovery_tgt = nullptr, // Deprecated *DO NOT USE*
        .init_queue = init_queue,
        .deinit_queue = deinit_queue,
        .reserved = {0, 0, 0, 0, 0} // Reserved
    });

    TLOGD("Starting {} [uuid:{}]", static_pointer_cast< UblkDisk >(device), to_string(vol_id))
    tgt->dev_data = std::make_unique< ublksrv_dev_data >(ublksrv_dev_data{
        .dev_id = device_id,
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
    if (!res) {
        tgt->destroy();
        return std::unexpected(res.error());
    }
    tgt->device_path = res.value();

    auto new_tgt = new ublkpp_tgt(tgt);
    return std::unique_ptr< ublkpp_tgt >(new_tgt);
}

ublkpp_tgt::ublkpp_tgt(std::shared_ptr< ublkpp_tgt_impl > p) : _p(p) {}

ublkpp_tgt::~ublkpp_tgt() = default;

std::filesystem::path ublkpp_tgt::device_path() const { return _p->device_path; }
std::shared_ptr< UblkDisk > ublkpp_tgt::device() const { return _p->device; }
int ublkpp_tgt::device_id() const { return _p->dev_data->dev_id; }

void ublkpp_tgt::remove(std::unique_ptr< ublkpp_tgt > tgt) { tgt->_p->destroy(); }

void ublkpp_tgt_impl::destroy() {
    auto const str_id = fmt::format("Device {} [uuid:{}]", device_path.native(), to_string(volume_uuid));
    // First send a signal to stop the ublk device and exit all I/O queues
    if (ublk_dev) {
        TLOGD("Stopping {}", str_id)
        ublksrv_ctrl_stop_dev(ctrl_dev);
    }

    // Wait for all queue_handler threads to exit
    TLOGD("Waiting for I/O to stop on {}", str_id)
    for (auto& q : queue_handlers)
        q.join();

    // De-allocate the ublksrv device and free all unowned memory
    if (ublk_dev) {
        TLOGD("De-allocate {}", str_id)
        ublksrv_dev_deinit(ublk_dev);
        ublk_dev = nullptr;
    }

    // De-allocate our devices now, will cause things like RAID-1 to flush Bitmaps
    // and all FSDisk will close their fd's
    device.reset();

    // Delete the ublk control object (ublkc must be closed!)
    if (device_added) {
        TLOGD("Stopping Control for {}", str_id)
        ublksrv_ctrl_del_dev(ctrl_dev);
    }

    // De-allocate the ublksrv control device finally
    if (ctrl_dev) {
        TLOGD("De-allocate Control for {}", str_id)
        ublksrv_ctrl_deinit(ctrl_dev);
    }
    TLOGI("Stopped {}", str_id)
}

ublkpp_tgt_impl::~ublkpp_tgt_impl() {
    // Destructor intentionally left empty - call destroy() explicitly
}

} // namespace ublkpp
