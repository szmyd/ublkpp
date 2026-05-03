#pragma once

#include <filesystem>
#include <memory>
#include <string>

namespace ublkpp {

class ublk_disk;
using disk_handle = std::shared_ptr< ublk_disk >;

// Construct a file/block-backed disk against the given path.
// `parent_id` is woven into the metrics labels; pass empty if metrics correlation is not needed.
// Throws std::runtime_error on open / fstat / ioctl probe failure.
disk_handle make_fs_disk(std::filesystem::path const& path, std::string const& parent_id = "");

} // namespace ublkpp
