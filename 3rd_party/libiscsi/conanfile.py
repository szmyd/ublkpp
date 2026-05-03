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
        "fPIC": ['True', 'False'],
        "md5_provider": ['False', 'libgcrypt', 'gnutls'],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "md5_provider": 'False',
    }

    def requirements(self):
        if self.options.md5_provider == 'gnutls':
            self.requires("gnutls/3.8.7")
        elif self.options.md5_provider == 'libgcrypt':
            self.requires("libgcrypt/1.10.3")

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

        # iSER (iSCSI over RDMA) is auto-detected purely from header presence in libiscsi's
        # configure.ac — the AC_CACHE_CHECK does not link-test, so when /usr/include/infiniband
        # exists the iser.o is built but ibv_*/rdma_* symbols are unresolved at link time.
        # Override the autoconf cache so the check returns "no" and iser.c stays out.
        e.define("libiscsi_cv_HAVE_LINUX_ISER", "no")

        # MD5 backend: libiscsi's --with-{gnutls,libgcrypt} flags select which library
        # provides MD5 for CHAP login. False uses libiscsi's built-in (NEED_MD5=yes).
        if self.options.md5_provider == 'False':
            tc.configure_args.append("--without-gnutls")
            tc.configure_args.append("--without-libgcrypt")
        elif self.options.md5_provider == 'gnutls':
            tc.configure_args.append("--with-gnutls")
            tc.configure_args.append("--without-libgcrypt")
        elif self.options.md5_provider == 'libgcrypt':
            tc.configure_args.append("--without-gnutls")
            tc.configure_args.append("--with-libgcrypt")

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
