set -eu

echo -n "Exporting custom recipes..."
echo -n "ublksrv..."
conan export 3rd_party/ublksrv --version nbi.1.5.0 >/dev/null
echo "done."
