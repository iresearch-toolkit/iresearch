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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"
#include "tests_param.hpp"

#include "store/memory_directory.hpp"
#include "store/store_utils.hpp"
#include "utils/encryption.hpp"

NS_LOCAL

using irs::bstring;

void assert_encryption(size_t block_size, size_t header_lenght) {
  tests::rot13_encryption enc(block_size, header_lenght);

  bstring encrypted_header;
  encrypted_header.resize(enc.header_length());
  ASSERT_TRUE(enc.create_header("encrypted", &encrypted_header[0]));
  ASSERT_EQ(header_lenght, enc.header_length());

  bstring header = encrypted_header;
  auto cipher = enc.create_stream("encrypted", &header[0]);
  ASSERT_NE(nullptr, cipher);
  ASSERT_EQ(block_size, cipher->block_size());

  // unencrypted part of the header: counter+iv
  ASSERT_EQ(
    irs::bytes_ref(encrypted_header.c_str(), 2*cipher->block_size()),
    irs::bytes_ref(header.c_str(), 2*cipher->block_size())
  );

  // encrypted part of the header
  ASSERT_TRUE(
    encrypted_header.size() == 2*cipher->block_size()
    || (irs::bytes_ref(encrypted_header.c_str()+2*cipher->block_size(), encrypted_header.size() - 2*cipher->block_size())
        != irs::bytes_ref(header.c_str()+2*cipher->block_size(), header.size() - 2*cipher->block_size()))
  );

  const bstring data(
    reinterpret_cast<const irs::byte_type*>("4jLFtfXSuSdsGXbXqH8IpmPqx5n6IWjO9Pj8nZ0yD2ibKvZxPdRaX4lNsz8N"),
    30
  );

  // encrypt less than block size
  {
    bstring source(data.c_str(), 7);

    {
      size_t offset = 0;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }

    {
      size_t offset = 4;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }

    {
      size_t offset = 1023;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }
  }

  // encrypt size of the block
  {
    bstring source(data.c_str(), 13);

    {
      size_t offset = 0;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }

    {
      size_t offset = 4;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }

    {
      size_t offset = 1023;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }
  }

  // encrypt more than size of the block
  {
    bstring source = data;

    {
      size_t offset = 0;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }

    {
      size_t offset = 4;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }

    {
      size_t offset = 1023;
      ASSERT_TRUE(cipher->encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(
        irs::bytes_ref(data.c_str(), source.size()),
        irs::bytes_ref(source)
      );
    }
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                          ctr_encryption_test_case
// -----------------------------------------------------------------------------

TEST(ctr_encryption, static_consts) {
  ASSERT_EQ(4096, size_t(irs::ctr_encryption::DEFAULT_HEADER_LENGTH));
  ASSERT_EQ(sizeof(uint64_t), size_t(irs::ctr_encryption::MIN_HEADER_LENGTH));
}

TEST(ctr_encryption, create_header_stream) {
  assert_encryption(1, irs::ctr_encryption::DEFAULT_HEADER_LENGTH);
  assert_encryption(13, irs::ctr_encryption::DEFAULT_HEADER_LENGTH);
  assert_encryption(16, irs::ctr_encryption::DEFAULT_HEADER_LENGTH);
  assert_encryption(1, sizeof(uint64_t));
  assert_encryption(4, sizeof(uint64_t));
  assert_encryption(8, 2*sizeof(uint64_t));

  // block_size == 0
  {
    tests::rot13_encryption enc(0);

    bstring encrypted_header;
    ASSERT_FALSE(enc.create_header("encrypted", &encrypted_header[0]));
    ASSERT_FALSE(enc.create_stream("encrypted", &encrypted_header[0]));
  }

  // header is too small (< MIN_HEADER_LENGTH)
  {
    tests::rot13_encryption enc(1, 7);

    bstring encrypted_header;
    encrypted_header.resize(enc.header_length());
    ASSERT_FALSE(enc.create_header("encrypted", &encrypted_header[0]));
    ASSERT_FALSE(enc.create_stream("encrypted", &encrypted_header[0]));
  }

  // header is too small (< 2*block_size)
  {
    tests::rot13_encryption enc(13, 25);

    bstring encrypted_header;
    encrypted_header.resize(enc.header_length());
    ASSERT_FALSE(enc.create_header("encrypted", &encrypted_header[0]));
    ASSERT_FALSE(enc.create_stream("encrypted", &encrypted_header[0]));
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                              encryption_test_case
// -----------------------------------------------------------------------------

class encryption_test_case : public tests::directory_test_case_base { };

//TEST_P(encryption_test_case, encrypted_io) {
//  dir().attributes().emplace<tests::rot13_encryption>(13);
//
//  {
//
//  }
//
//
//
//  // unencrypted + encrypted data
//  {
//    // write data
//    {
//      auto out = dir_->create("encrypted");
//      irs::write_string(*out, irs::string_ref("header"));
//      ASSERT_EQ(7, out->file_pointer());
//
//      irs::encrypted_output encryptor(std::move(out), cipher, 10);
//      ASSERT_EQ(10*cipher.block_size(), encryptor.buffer_size());
//      ASSERT_EQ(0, encryptor.file_pointer());
//      irs::write_string(encryptor, irs::string_ref("encrypted header"));
//      ASSERT_EQ(7, encryptor.stream().file_pointer());
//      ASSERT_EQ(17, encryptor.file_pointer());
//      ASSERT_THROW(encryptor.flush(), irs::io_error); // length is not multiple of cipher block size
//      encryptor.append_and_flush();
//      ASSERT_EQ(26, encryptor.file_pointer());
//      ASSERT_EQ(33, encryptor.stream().file_pointer());
//    }
//
//    // read data
//    {
//      auto in = dir_->open("encrypted", IOAdvice::NORMAL);
//      ASSERT_EQ("header", irs::read_string<std::string>(*in));
//      irs::encrypted_input decryptor(std::move(in), cipher, 13);
//      ASSERT_EQ(decryptor.stream().length()-7, decryptor.length());
//      ASSERT_EQ(2*cipher.block_size(), decryptor.length());
//      ASSERT_EQ(13*cipher.block_size(), decryptor.buffer_size());
//      ASSERT_EQ(0, decryptor.file_pointer());
//      ASSERT_EQ("encrypted header", irs::read_string<std::string>(decryptor));
//      ASSERT_EQ(17, decryptor.file_pointer());
//      ASSERT_EQ(decryptor.stream().length(), decryptor.stream().file_pointer());
//
//      // check padding
//      while (decryptor.file_pointer() < decryptor.length()) {
//        ASSERT_EQ(0, decryptor.read_byte());
//      }
//    }
//  }
//
//}
//
//  // data is shorter than ciper block size
//  {
//    // write data
//    {
//      irs::encrypted_output encryptor(dir_->create("encrypted_small"), cipher, 0);
//      ASSERT_EQ(cipher.block_size(), encryptor.buffer_size());
//      ASSERT_EQ(0, encryptor.file_pointer());
//      ASSERT_EQ(0, encryptor.stream().file_pointer());
//      irs::write_string(encryptor, irs::string_ref("header"));
//      ASSERT_EQ(0, encryptor.stream().file_pointer());
//      ASSERT_EQ(7, encryptor.file_pointer());
//      ASSERT_THROW(encryptor.flush(), irs::io_error); // length is not multiple of cipher block size
//      encryptor.append_and_flush();
//      ASSERT_EQ(13, encryptor.file_pointer());
//      ASSERT_EQ(encryptor.file_pointer(), encryptor.stream().file_pointer());
//    }
//
//    // read data
//    {
//      irs::encrypted_input decryptor(dir_->open("encrypted_small", IOAdvice::NORMAL), cipher, 0);
//      ASSERT_EQ(decryptor.stream().length(), decryptor.length());
//      ASSERT_EQ(cipher.block_size(), decryptor.length());
//      ASSERT_EQ(cipher.block_size(), decryptor.buffer_size());
//      ASSERT_EQ(0, decryptor.file_pointer());
//      ASSERT_EQ("header", irs::read_string<std::string>(decryptor));
//      ASSERT_EQ(7, decryptor.file_pointer());
//
//      // check padding
//      while (decryptor.file_pointer() < decryptor.length()) {
//        ASSERT_EQ(0, decryptor.read_byte());
//      }
//
//      ASSERT_EQ(decryptor.stream().length(), decryptor.stream().file_pointer());
//    }
//  }
//
//  // data length is equal to ciper block size
//  {
//    // write data
//    {
//      irs::encrypted_output encryptor(dir_->create("encrypted_equal_to_block_size"), cipher, 0);
//      ASSERT_EQ(cipher.block_size(), encryptor.buffer_size());
//      ASSERT_EQ(0, encryptor.file_pointer());
//      ASSERT_EQ(0, encryptor.stream().file_pointer());
//      irs::write_string(encryptor, irs::string_ref("headerheader"));
//      ASSERT_EQ(cipher.block_size(), encryptor.stream().file_pointer());
//      ASSERT_EQ(cipher.block_size(), encryptor.file_pointer());
//      encryptor.flush(); // length is multiple of cipher block size
//      ASSERT_EQ(cipher.block_size(), encryptor.file_pointer());
//      ASSERT_EQ(encryptor.file_pointer(), encryptor.stream().file_pointer());
//    }
//
//    // read data
//    {
//      irs::encrypted_input decryptor(dir_->open("encrypted_equal_to_block_size", IOAdvice::NORMAL), cipher, 0);
//      ASSERT_EQ(decryptor.stream().length(), decryptor.length());
//      ASSERT_EQ(cipher.block_size(), decryptor.length());
//      ASSERT_EQ(cipher.block_size(), decryptor.buffer_size());
//      ASSERT_EQ(0, decryptor.file_pointer());
//      ASSERT_EQ("headerheader", irs::read_string<std::string>(decryptor));
//      ASSERT_EQ(cipher.block_size(), decryptor.file_pointer());
//      ASSERT_EQ(decryptor.length(), decryptor.file_pointer());
//      ASSERT_EQ(decryptor.stream().length(), decryptor.stream().file_pointer());
//    }
//  }
//
//  // data length is longer than ciper block size
//  {
//    // write data
//    {
//      irs::encrypted_output encryptor(dir_->create("encrypted_longer_than_block_size"), cipher, 0);
//      ASSERT_EQ(cipher.block_size(), encryptor.buffer_size());
//      ASSERT_EQ(0, encryptor.file_pointer());
//      ASSERT_EQ(0, encryptor.stream().file_pointer());
//      irs::write_string(encryptor, irs::string_ref("headerheaderh"));
//      ASSERT_EQ(cipher.block_size(), encryptor.stream().file_pointer());
//      ASSERT_EQ(14, encryptor.file_pointer());
//      ASSERT_THROW(encryptor.flush(), irs::io_error); // length is not multiple of cipher block size
//      encryptor.append_and_flush();
//      ASSERT_EQ(2*cipher.block_size(), encryptor.file_pointer());
//      ASSERT_EQ(encryptor.file_pointer(), encryptor.stream().file_pointer());
//    }
//
//    // read data
//    {
//      irs::encrypted_input decryptor(dir_->open("encrypted_longer_than_block_size", IOAdvice::NORMAL), cipher, 0);
//      ASSERT_EQ(2*cipher.block_size(), decryptor.length());
//      ASSERT_EQ(decryptor.stream().length(), decryptor.length());
//      ASSERT_EQ(cipher.block_size(), decryptor.buffer_size());
//      ASSERT_EQ(0, decryptor.file_pointer());
//      ASSERT_EQ("headerheaderh", irs::read_string<std::string>(decryptor));
//      ASSERT_EQ(14, decryptor.file_pointer());
//      ASSERT_EQ(decryptor.stream().length(), decryptor.stream().file_pointer());
//
//      // check padding
//      while (decryptor.file_pointer() < decryptor.length()) {
//        ASSERT_EQ(0, decryptor.read_byte());
//      }
//
//      ASSERT_EQ(decryptor.length(), decryptor.file_pointer());
//    }
//  }
//
//  // long encrypted stream, small buffer
//  {
//    // write data
//    {
//      irs::encrypted_output encryptor(dir_->create("encrypted_long_string"), cipher, 0);
//      ASSERT_EQ(cipher.block_size(), encryptor.buffer_size());
//      ASSERT_EQ(0, encryptor.file_pointer());
//      ASSERT_EQ(0, encryptor.stream().file_pointer());
//      irs::write_string(encryptor, irs::string_ref("headerheaderheaderheaderheader"));
//      ASSERT_EQ(2*cipher.block_size(), encryptor.stream().file_pointer());
//      ASSERT_EQ(31, encryptor.file_pointer());
//      ASSERT_THROW(encryptor.flush(), irs::io_error); // length is not multiple of cipher block size
//      encryptor.append_and_flush();
//      ASSERT_EQ(3*cipher.block_size(), encryptor.file_pointer());
//      ASSERT_EQ(encryptor.file_pointer(), encryptor.stream().file_pointer());
//    }
//
//    // read data
//    {
//      irs::encrypted_input decryptor(dir_->open("encrypted_long_string", IOAdvice::NORMAL), cipher, 0);
//      ASSERT_EQ(3*cipher.block_size(), decryptor.length());
//      ASSERT_EQ(cipher.block_size(), decryptor.buffer_size());
//      ASSERT_EQ(0, decryptor.file_pointer());
//      ASSERT_EQ("headerheaderheaderheaderheader", irs::read_string<std::string>(decryptor));
//      ASSERT_EQ(decryptor.stream().length(), decryptor.stream().file_pointer());
//      ASSERT_EQ(31, decryptor.file_pointer());
//
//      // check padding
//      while (decryptor.file_pointer() < decryptor.length()) {
//        ASSERT_EQ(0, decryptor.read_byte());
//      }
//    }
//  }
//
//  // long encrypted stream, small buffer, blob is equal to cipher block size
//  {
//    const auto data = irs::ref_cast<irs::byte_type>(irs::string_ref("012345678912301234567891230123456789123"));
//    ASSERT_EQ(3*cipher.block_size(), data.size());
//
//    // write data
//    {
//      irs::encrypted_output encryptor(dir_->create("encrypted_long_string_multiple_to_block_size"), cipher, 0);
//      ASSERT_EQ(cipher.block_size(), encryptor.buffer_size());
//      ASSERT_EQ(0, encryptor.file_pointer());
//      ASSERT_EQ(0, encryptor.stream().file_pointer());
//      encryptor.write_bytes(data.c_str(), data.size());
//      ASSERT_EQ(3*cipher.block_size(), encryptor.stream().file_pointer());
//      ASSERT_EQ(39, encryptor.file_pointer());
//      encryptor.flush();
//      ASSERT_EQ(3*cipher.block_size(), encryptor.file_pointer());
//      ASSERT_EQ(encryptor.file_pointer(), encryptor.stream().file_pointer());
//    }
//
//    // read data
//    {
//      irs::byte_type read[39];
//
//      irs::encrypted_input decryptor(dir_->open("encrypted_long_string_multiple_to_block_size", IOAdvice::NORMAL), cipher, 0);
//      ASSERT_EQ(3*cipher.block_size(), decryptor.length());
//      ASSERT_EQ(cipher.block_size(), decryptor.buffer_size());
//      ASSERT_EQ(0, decryptor.file_pointer());
//      decryptor.read_bytes(read, sizeof read);
//      ASSERT_EQ(data, irs::bytes_ref(read, sizeof read));
//      ASSERT_EQ(decryptor.stream().length(), decryptor.stream().file_pointer());
//      ASSERT_EQ(39, decryptor.file_pointer());
//    }
//  }
//
//  // long encrypted stream
//  {
//    // write data
//    {
//      irs::encrypted_output encryptor(dir_->create("encrypted_long"), cipher, 0);
//      ASSERT_EQ(cipher.block_size(), encryptor.buffer_size());
//      ASSERT_EQ(0, encryptor.file_pointer());
//      ASSERT_EQ(0, encryptor.stream().file_pointer());
//      for (size_t step = 97, seed = 99, i = 0; i < 10000; ++i, seed += step) {
//        encryptor.write_long(seed);
//        encryptor.write_vlong(seed);
//        const auto str = std::to_string(seed);
//        irs::write_string(encryptor, str);
//      }
//      encryptor.append_and_flush();
//      ASSERT_EQ(encryptor.file_pointer(), encryptor.stream().file_pointer());
//    }
//
//    // read data
//    {
//      irs::byte_type read[39];
//
//      irs::encrypted_input decryptor(dir_->open("encrypted_long", IOAdvice::NORMAL), cipher, 0);
//      ASSERT_EQ(cipher.block_size(), decryptor.buffer_size());
//      ASSERT_EQ(0, decryptor.file_pointer());
//      for (size_t step = 97, seed = 99, i = 0; i < 10000; ++i, seed += step) {
//        ASSERT_EQ(seed, decryptor.read_long());
//        ASSERT_EQ(seed, decryptor.read_vlong());
//        const auto expected_str = std::to_string(seed);
//        auto str = irs::read_string<std::string>(decryptor);
//        ASSERT_EQ(expected_str, str);
//      }
//      // check padding
//      while (decryptor.file_pointer() < decryptor.length()) {
//        ASSERT_EQ(0, decryptor.read_byte());
//      }
//      ASSERT_EQ(decryptor.stream().length(), decryptor.stream().file_pointer());
//    }
//  }
//
//  // FIXME
//  // - try to avoid copying data into buffered stream buffer in case if encrypted buffer size matches underlying buffer size
//  // - test cipher with block size == 0
//  // - test format with different cipher block sizes (e.g. 13, 16, 7)
//  // - test different block sizes and underlying stream buffer sizes
//  // - extend index tests to use encrypted format
//  // - fixme test block size < sizeof(uint64_t)/2
//  // - write/read checksum over unencrypted data checksum
//  // - test extreme values of block_size/header_length for ctr encryption
//}

INSTANTIATE_TEST_CASE_P(
  encryption_test,
  encryption_test_case,
  ::testing::Values(
    &tests::memory_directory,
    &tests::fs_directory,
    &tests::mmap_directory
  ),
  tests::directory_test_case_base::to_string
);

NS_END
