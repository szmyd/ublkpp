from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake, cmake_layout
from conan.tools.files import copy
from conan.tools.files import copy
from os.path import join

required_conan_version = ">=1.60.0"

class UBlkPPConan(ConanFile):
    name = "ublkpp"
    version = "0.8.5"

    homepage = "https://github.com/szmyd/ublkpp"
    description = "A UBlk library for CPP application"
    topics = ("ublk")
    url = "https://github.com/szmyd/ublkpp"
    license = "Apache-2.0"

    settings = "arch", "os", "compiler", "build_type"

    options = {
                "shared": ['True', 'False'],
                "fPIC": ['True', 'False'],
                "coverage": ['True', 'False'],
                "sanitize": ['True', 'False'],
                "homeblocks": ['True', 'False'],
                "iscsi": ['True', 'False'],
                }
    default_options = {
                'shared': False,
                'fPIC': True,
                'coverage': False,
                'sanitize': False,
                'homeblocks': False,
                'iscsi': True,
            }

    exports_sources = (
                        "CMakeLists.txt",
                        "cmake/*",
                        "include/*",
                        "example/*",
                        "src/*",
                        "LICENSE"
                        )

    def _min_cppstd(self):
        return 20

    def validate(self):
        if self.settings.compiler.get_safe("cppstd"):
            check_min_cppstd(self, self._min_cppstd())

    def config_options(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
            if self.conf.get("tools.build:skip_test", default=False):
                if self.options.coverage or self.options.sanitize:
                    raise ConanInvalidConfiguration("Coverage/Sanitizer requires Testing!")

    def configure(self):
        if self.settings.build_type != "Debug":
            self.options['sisl/*'].malloc_impl = 'tcmalloc'

    def build_requirements(self):
        self.test_requires("gtest/1.15.0")

    def requirements(self):
        self.requires("sisl/[^12.3]@oss/master", transitive_headers=True)

        self.requires("isa-l/2.30.0")
        if (self.options.get_safe("homeblocks")):
            self.requires("homeblocks/[^2.1]@oss/main")
        self.requires("ublksrv/nbi.1.5.0")
        if (self.options.get_safe("iscsi")):
            self.requires("libiscsi/1.20.2")

    def layout(self):
        self.folders.source = "."
        if self.options.get_safe("sanitize"):
            self.folders.build = join("build", "Sanitized")
        elif self.options.get_safe("coverage"):
            self.folders.build = join("build", "Coverage")
        else:
            self.folders.build = join("build", str(self.settings.build_type))
        self.folders.generators = join(self.folders.build, "generators")

        self.cpp.source.includedirs = ["include"]

        self.cpp.build.libdirs = ["src"]

        self.cpp.package.libs = ["ublkpp"]
        self.cpp.package.includedirs = ["include"] # includedirs is already set to 'include' by
        self.cpp.package.libdirs = ["lib"]

    def generate(self):
        # This generates "conan_toolchain.cmake" in self.generators_folder
        tc = CMakeToolchain(self)
        tc.variables["CTEST_OUTPUT_ON_FAILURE"] = "ON"
        tc.variables["PACKAGE_VERSION"] = self.version
        tc.variables["ENABLE_TESTS"] = "ON"
        if self.conf.get("tools.build:skip_test", default=False):
            tc.variables["ENABLE_TESTS"] = "OFF"
        if self.settings.build_type == "Debug":
            if self.options.get_safe("coverage"):
                tc.variables['BUILD_COVERAGE'] = 'ON'
            elif self.options.get_safe("sanitize"):
                tc.variables['MEMORY_SANITIZER_ON'] = 'ON'
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        if not self.conf.get("tools.build:skip_test", default=False):
            cmake.test()

    def package(self):
        copy(self, "LICENSE", self.source_folder, join(self.package_folder, "licenses"), keep_path=False)
        copy(self, "*.h*", join(self.source_folder, "include"), join(self.package_folder, "include"), keep_path=True)
        copy(self, "*.a", self.build_folder, join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.so", self.build_folder, join(self.package_folder, "lib"), keep_path=False)

    def package_info(self):
        if self.options.get_safe("sanitize"):
            self.cpp_info.sharedlinkflags.append("-fsanitize=address")
            self.cpp_info.exelinkflags.append("-fsanitize=address")
            self.cpp_info.sharedlinkflags.append("-fsanitize=undefined")
            self.cpp_info.exelinkflags.append("-fsanitize=undefined")

        self.cpp_info.set_property("cmake_file_name", "UblkPP")
        self.cpp_info.set_property("cmake_target_name", "UblkPP::UblkPP")
        self.cpp_info.names["cmake_find_package"] = "UblkPP"
        self.cpp_info.names["cmake_find_package_multi"] = "UblkPP"
