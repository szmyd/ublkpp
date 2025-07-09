set -eu

echo -n "Exporting custom recipes..."
echo -n "ublksrv..."
conan export 3rd_party/ublksrv ublksrv/nbi.1.5.0@
conan export 3rd_party/libiscsi libiscsi/1.20.2@
echo "done."
