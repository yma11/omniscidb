#! /bin/bash
source /usr/local/mapd-deps/mapd-deps.sh

cd /omniscidb/
mkdir -p build_docker
cd build_docker

cp ../scripts/build.sh ./

echo "!!!!!! start build !!!!!!"
/bin/bash ./build.sh

echo "!!!!!! start test !!!!!!"
cd Tests
mkdir -p tmp
../bin/initdb --data ./tmp

ctest -V