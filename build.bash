DEPS_DIR="$(pwd)/../iresearch.deps"
export BISON_ROOT="${DEPS_DIR}/bison/3.0.4"
export BOOST_ROOT="${DEPS_DIR}/boost/1.71.0"
export GTEST_ROOT="${DEPS_DIR}/gtest/googletest"
export ICU_ROOT="${DEPS_DIR}/icu/65.1/build"
export LZ4_ROOT="${DEPS_DIR}/lz4"
export SNOWBALL_ROOT="${DEPS_DIR}/snowball"
#export VPACK_ROOT="$(pwd)/external/velocypack"
export UNWIND_ROOT=invalid # FIXME needs liblzma.so in Ubuntu
PATH=${PATH}:${BISON_ROOT}/bin
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$(pwd)/build/bin"
rm -rf build
mkdir build
cd build
cmake -DCMAKE_CXX_STANDARD=20 -DCMAKE_BUILD_TYPE=Debug -DUSE_TESTS=On -DUSE_IPO=Off -G "Unix Makefiles" ..

make -j $(nproc) iresearch-tests-static
