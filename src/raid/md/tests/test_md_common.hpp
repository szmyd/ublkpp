#pragma once

extern "C" {
#include <endian.h>
}

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <ublk_cmd.h>
#include <ublksrv.h>

#include "ublkpp/lib/ublk_disk.hpp"

#include "lib/common.hpp"

namespace ublkpp::md::test {

// md superblock 1.2 builder. Defaults produce a clean, near=2 RAID-10 leg.
struct MdSbBuilder {
    uint32_t magic{0xa92b4efc};
    uint32_t major_version{1};
    uint32_t feature_map{0};
    uint8_t set_uuid[16]{};
    uint32_t level{10};
    uint32_t layout{0x00010002};     // near_copies=2, far_copies=0, far_offset=0
    uint32_t chunksize_sectors{128}; // 64 KiB / 512
    uint32_t raid_disks{4};
    uint64_t data_offset_sectors{262144}; // 128 MiB / 512
    uint32_t dev_number{0};
    uint64_t events{42};
    uint64_t resync_offset{~uint64_t{0}}; // clean
    uint16_t dev_roles[256]{0, 1, 2, 3};

    MdSbBuilder() {
        // Distinct repeatable set_uuid so cross-leg checks pass.
        for (size_t i = 0; i < 16; ++i)
            set_uuid[i] = static_cast< uint8_t >(0xa0 + i);
    }

    void emit(uint8_t* page) const {
        std::memset(page, 0, 4 * Ki);
        auto put32 = [&](size_t off, uint32_t v) {
            v = htole32(v);
            std::memcpy(page + off, &v, sizeof(v));
        };
        auto put64 = [&](size_t off, uint64_t v) {
            v = htole64(v);
            std::memcpy(page + off, &v, sizeof(v));
        };
        auto put16 = [&](size_t off, uint16_t v) {
            v = htole16(v);
            std::memcpy(page + off, &v, sizeof(v));
        };

        put32(0x00, magic);
        put32(0x04, major_version);
        put32(0x08, feature_map);
        std::memcpy(page + 0x10, set_uuid, 16);
        put32(0x48, level);
        put32(0x4C, layout);
        put32(0x58, chunksize_sectors);
        put32(0x5C, raid_disks);
        put64(0x80, data_offset_sectors);
        put32(0xA0, dev_number);
        put64(0xC8, events);
        put64(0xD0, resync_offset);
        for (uint32_t i = 0; i < raid_disks && i < 256; ++i)
            put16(0x100 + 2 * i, dev_roles[i]);
    }
};

// In-memory backing for a leaf. Behaves like a regular block device via sync_iov; async_iov
// is not exercised by MdDisk's own SB read/write or by the discovered_topo parser. RAID1 above
// us issues async_iov but we don't unit-test that path here.
class BufferedDisk : public ublk_disk {
public:
    BufferedDisk(uint64_t capacity, std::string id, uint32_t logical_bs = ublkpp::DEFAULT_BLOCK_SIZE) :
            ublk_disk(), _id(std::move(id)) {
        _buf.assign(capacity, 0xa5); // distinct fill so unwritten reads are noticeable
        auto& p = *params();
        p.basic.dev_sectors = capacity >> SECTOR_SHIFT;
        p.basic.logical_bs_shift = ilog2(logical_bs);
        p.basic.physical_bs_shift = p.basic.logical_bs_shift;
        p.basic.max_sectors = (512 * Ki) >> SECTOR_SHIFT;
        p.types |= UBLK_PARAM_TYPE_DISCARD;
        _direct_io = true;
    }

    std::string id() const noexcept override { return _id; }

    uint8_t* data() noexcept { return _buf.data(); }
    uint8_t const* data() const noexcept { return _buf.data(); }
    size_t size() const noexcept { return _buf.size(); }

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept override {
        if (addr < 0) return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
        size_t off = static_cast< size_t >(addr);
        size_t total = 0;
        for (uint32_t i = 0; i < nr_vecs; ++i) {
            if (off + iovecs[i].iov_len > _buf.size())
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            if (op == UBLK_IO_OP_READ) {
                std::memcpy(iovecs[i].iov_base, _buf.data() + off, iovecs[i].iov_len);
            } else if (op == UBLK_IO_OP_WRITE) {
                std::memcpy(_buf.data() + off, iovecs[i].iov_base, iovecs[i].iov_len);
            } else {
                return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
            }
            off += iovecs[i].iov_len;
            total += iovecs[i].iov_len;
        }
        return total;
    }

private:
    std::string _id;
    std::vector< uint8_t > _buf;
};

// Stage an md 1.2 SB at byte 4 KiB on a BufferedDisk. Returns the SB builder used so callers
// can mutate fields and re-stage if needed.
inline MdSbBuilder stage_md_sb(BufferedDisk& d, MdSbBuilder builder = {}) {
    builder.emit(d.data() + 4 * Ki);
    return builder;
}

// The default set_uuid baked into MdSbBuilder, as a boost::uuids::uuid. Tests that stage
// legs with the builder's defaults should pass this value as the volume uuid to make_md_*
// factories so MdDisk's verification succeeds.
inline boost::uuids::uuid const& default_md_uuid() {
    static boost::uuids::uuid const u{
        {0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf}};
    return u;
}

// Fill a region with a deterministic byte pattern so cross-translation tests can verify
// that ublkpp reads the same bytes md wrote.
inline void fill_pattern(uint8_t* p, size_t len, uint64_t seed) {
    std::mt19937_64 rng(seed);
    for (size_t i = 0; i < len; i += sizeof(uint64_t)) {
        uint64_t v = rng();
        std::memcpy(p + i, &v, std::min(sizeof(uint64_t), len - i));
    }
}

} // namespace ublkpp::md::test
