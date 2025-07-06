# ublkpp

## Intro

A target for Linux' [userspace block](https://docs.kernel.org/block/ublk.html) driver, currently
based on [ublksrv](https://github.com/ublk-org/ublksrv) IO_URING implementation.

## QuickStart

### Build Library

    $ git clone git@github.com:szmyd/ublkpp
    $ ublkpp/prepare_v2.sh
    $ conan build -s:h build_type=Debug --build missing ublkpp

## Example App
An example application lives under the `test_package` directory. It can be used to try out basic RAID1/0/10 capabilities
with a single Target. The lifetime of the ublk device is tied to the lifetime of the process itself.

### Build and Run Server Application

    $ conan create -s:h build_type=Release --build missing ublkpp
    $ sudo modprobe ublk_drv
    $ fallocate -l 2G file1.dat
    $ fallocate -l 2G file2.dat
    $ fallocate -l 2G file3.dat
    $ fallocate -l 2G file4.dat
    $ hexdump -n 256 file*.dat
        0000000 0000 0000 0000 0000 0000 0000 0000 0000
        *
        0000100
    $ sudo ublkpp/test_package/build/Debug/ublkpp_disk -cv 2 --raid1 file1.dat,file2.dat,file3.dat,file4.dat
        ... // SuperBlock Initialization
        [07/06/25 18:01:50] [info] [test_package/build/Debug/ublkpp_disk] [223787] [ublkpp_tgt.cpp:178:start] Device exposed as UBD device: [/dev/ublkb0]

### Usage
In another session we should find the exposed BLOCK device.

    $ lsblk
        NAME        MAJ:MIN RM  SIZE RO TYPE MOUNTPOINTS
        ...
        ublkb0      259:3    0    4G  0 disk

### Write Data into ublk Device

    $ sudo dd if=/dev/urandom of=/dev/ublkb0 bs=4k count=1 
        1+0 records in
        1+0 records out
        4096 bytes copied, 0.00407932 s, 126 kB/s
    $ hexdump -n 256 file1.dat
        0000000 2553 0aff 9934 c53e 3a67 17c8 ae49 641b
        0000010 0100 a0d8 b2c5 e056 7144 61af 0854 461f
        0000020 4c0f 0001 0000 0000 0000 0000 0080 0001
        0000030 0000 0001 0000 0000 0000 0000 0000 0000
        0000040 0000 0000 0000 0000 0000 0000 0000 0000
        *
        0000100

If we dump the first 256B of any backing device we'll find the RAID-0 SuperBlock for the first Stripe.

## License Information
Primary Author: [Brian Szmyd](https://github.com/szmyd)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITHomeStore OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
