#include "md_superblock.hpp"

namespace ublkpp::md {

// 128-bit magic. Independent of raid0/raid1 magic constants by inspection.
uint8_t const k_magic[k_magic_size] = {0x6d, 0x64, 0x70, 0x70, 0x53, 0x42, 0x00, 0x01,
                                       0xfe, 0xed, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef};

} // namespace ublkpp::md
