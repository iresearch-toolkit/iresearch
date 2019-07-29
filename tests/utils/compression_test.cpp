////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "utils/lz4compression.hpp"

#include <numeric>
#include <random>

using namespace iresearch;

TEST(compression_test, lz4) {
  using namespace iresearch;

  compression::lz4decompressor decompressor;
  compression::lz4compressor compressor;
  ASSERT_EQ(0, compressor.acceleration());

  std::vector<size_t> data(2047, 0);

  std::random_device rnd_device;
  std::mt19937 mersenne_engine {rnd_device()};
  std::uniform_int_distribution<size_t> dist {1, 52};
  auto generator = [&dist, &mersenne_engine](){ return dist(mersenne_engine); };

  for (size_t i = 0; i < 10; ++i) {
    std::generate(data.begin(), data.end(), generator);

    bstring compression_buf;
    const bytes_ref to_compress(reinterpret_cast<const byte_type*>(data.data()),
                                data.size()*sizeof(size_t));

    const auto compressed = compressor.compress(to_compress, compression_buf);

    bstring decompression_buf(to_compress.size(), 0); // ensure we have enough space in buffer
    const auto decompressed_size = decompressor.decompress(compressed.begin(), compressed.size(),
                                                           &decompression_buf[0], decompression_buf.size());

    ASSERT_EQ(to_compress.size(), decompressed_size);
    ASSERT_EQ(to_compress, decompression_buf);
  }
}
