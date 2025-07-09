from conan import ConanFile
from conan.tools.gnu import Autotools, AutotoolsToolchain, AutotoolsDeps
from conan.tools.files import apply_conandata_patches, export_conandata_patches, get, replace_in_file
from os.path import join

required_conan_version = ">=1.60.0"

class LibIScsiConan(ConanFile):
    name = "libiscsi"
    description = "Data Plane Development Kit"
    url = "https://github.com/sahlberg/libiscsi"
    homepage = "https://github.com/sahlberg/libiscsi"
    license = "BSD-3"
    exports = ["LICENSE"]
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": ['True', 'False'],
        "fPIC": ['True', 'False']
    }
    default_options = {
        "shared":False,
        "fPIC":True,
    }

    def requirements(self):
        self.requires("gnutls/3.8.7")

    def configure(self):
        del self.settings.compiler.libcxx

    def export_sources(self):
        export_conandata_patches(self)

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        tc = AutotoolsToolchain(self)
        e = tc.environment()
        e.append("CFLAGS", "-Wno-unused-but-set-variable")
        e.append("LIBGNUTLS_LIBS", "-luring".format(self.dependencies['gnutls'].package_folder))
        e.append("LIBGNUTlS_CFLAGS", "-I{}/include".format(self.dependencies['gnutls'].package_folder))

        if self.settings.build_type == "Debug":
            tc.configure_args.append("--enable-debug")
        tc.generate(e)

        td = AutotoolsDeps(self)
        td.generate()

    def build(self):
        apply_conandata_patches(self)
        autotools = Autotools(self)
        autotools.autoreconf()
        autotools.configure()
        autotools.make()

    def package(self):
        autotools = Autotools(self)
        autotools.install(args=["DESTDIR={}".format(self.package_folder)])

    def deploy(self):
        pass

    def package_info(self):
        self.cpp_info.libs = [
                              "iscsi",
                              ]
