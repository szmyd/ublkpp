# ublkpp

## Intro

A target for Linux' [userspace block](https://docs.kernel.org/block/ublk.html) driver, currently
based on [ublksrv](https://github.com/ublk-org/ublksrv) IO_URING implementation.

## QuickStart

### Build Library

    $ git clone git@github.com:szmyd/ublkpp
    $ ublkpp/prepare.sh
    $ conan build -s:h build_type=Debug --build missing ublkpp

## License Information
Primary Author: [Brian Szmyd](https://github.com/szmyd)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITHomeStore OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
