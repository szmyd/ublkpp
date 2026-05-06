set -eu

echo -n "Exporting custom recipes..."
echo -n "ublksrv..."
conan export 3rd_party/ublksrv --version nbi.1.5.0.1 >/dev/null
echo -n "fio..."
conan export 3rd_party/fio --version nbi.3.28 >/dev/null
echo "done."
