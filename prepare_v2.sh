set -eu

echo -n "Exporting custom recipes..."
echo -n "ublksrv..."
conan export 3rd_party/ublksrv --version nbi.1.5.0 >/dev/null
conan export 3rd_party/libiscsi --version 1.20.2 >/dev/null
echo "done."
