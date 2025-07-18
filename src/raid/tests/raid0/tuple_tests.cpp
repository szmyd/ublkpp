#include "test_raid0_common.hpp"

#define TEST_ACCESS(NR, SS, ADDR, LEN, DOFF, LOFF, SZ)                                                                 \
    {                                                                                                                  \
        auto const ss = (SS);                                                                                          \
        auto [d_off, l_off, sz] = ublkpp::raid0::next_subcmd(ss * (NR), ss, (ADDR), (LEN));                            \
        EXPECT_EQ((DOFF), d_off);                                                                                      \
        EXPECT_EQ((LOFF), l_off);                                                                                      \
        EXPECT_EQ((SZ), sz);                                                                                           \
    }

// Some some various outcomes of writes across a RAID stripe used by the I/O handlers
TEST(Raid0, CalcTuples) {
    // Access on first stripe of 3-device RAID, first chunk
    TEST_ACCESS(3, Ki, 0x0000, 512, 0, 0, 512);
    // Access on second stripe of 3-device RAID, third chunk
    TEST_ACCESS(3, Ki, ((Ki) * 6) + Ki + 512, 512, 1, (2 * Ki) + 512, 512);
    // Access on second stripe of 3-device RAID, second chunk, across third device
    TEST_ACCESS(3, 128 * Ki, ((128 * Ki) * 4) + Ki, 128 * Ki, 1, (129 * Ki), 127 * Ki);
}
