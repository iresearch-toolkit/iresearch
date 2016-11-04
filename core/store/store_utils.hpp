//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#ifndef IRESEARCH_STORE_UTILS_H
#define IRESEARCH_STORE_UTILS_H

#include "shared.hpp"

#include "directory.hpp"
#include "data_output.hpp"
#include "data_input.hpp"

#include "utils/string.hpp"
#include "utils/bit_utils.hpp"
#include "utils/numeric_utils.hpp"
#include "utils/attributes.hpp"

#include <unordered_set>
#include <cassert>

NS_LOCAL

using iresearch::data_input;
using iresearch::data_output;

template< typename SizeType >
struct size_helper { 
  static SizeType read( data_input& in );
  static SizeType write( data_output& out, size_t size );
};

template<>
struct size_helper< uint32_t > {
  inline static uint32_t read( data_input& in ) { 
    return in.read_vint(); 
  }
  
  inline static void write( data_output& out, size_t size ) {
    out.write_vint( static_cast<uint32_t>(size) );
  }
};

template<>
struct size_helper< uint64_t > {
  inline static uint64_t read( data_input& in ) { 
    return in.read_vlong(); 
  }
  
  inline static void write( data_output& out, size_t size ) {
    out.write_vlong( static_cast<uint64_t>(size) );
  }
};

NS_END // LOCAL

NS_ROOT

// ----------------------------------------------------------------------------
// --SECTION--                                               read/write helpers 
// ----------------------------------------------------------------------------

inline void write_size( data_output& out, size_t size ) {
  ::size_helper< size_t >::write( out, size );
}

inline size_t read_size( data_input& in ) {
  return ::size_helper< size_t >::read( in );
}

void IRESEARCH_API write_zvfloat(data_output& out, float_t v);

float_t IRESEARCH_API read_zvfloat(data_input& in);

void IRESEARCH_API write_zvdouble(data_output& out, double_t v);

double_t IRESEARCH_API read_zvdouble(data_input& in);

inline void write_zvint( data_output& out, int32_t v ) {
  out.write_vint( zig_zag_encode32( v ) );
}

inline int32_t read_zvint( data_input& in ) {
  return zig_zag_decode32( in.read_vint() );
}

inline void write_zvlong( data_output& out, int64_t v ) {
  out.write_vlong( zig_zag_encode64( v ) );
}

inline int64_t read_zvlong( data_input& in ) {
  return zig_zag_decode64( in.read_vlong() );
}

inline void write_string( data_output& out, const char* s, size_t len ) {
  assert(len < integer_traits<uint32_t>::const_max);
  out.write_vint(uint32_t(len));
  out.write_bytes(reinterpret_cast<const byte_type*>(s), len);
}

inline void write_string(data_output& out, const byte_type* s, size_t len) {
  assert(len < integer_traits<uint32_t>::const_max);
  out.write_vint(uint32_t(len));
  out.write_bytes(s, len);
}

template< typename StringType >
inline void write_string(data_output& out, const StringType& str) {
  write_string(out, str.c_str(), str.size());
}

template< typename ContType >
inline data_output& write_strings(data_output& out, const ContType& c) {
  write_size(out, c.size());
  for (const auto& s : c) {
    write_string< decltype(s) >(out, s);
  }

  return out;
}

template< typename StringType >
inline StringType read_string(data_input& in) {
  const size_t len = in.read_vint();

  StringType str(len, 0);
#ifdef IRESEARCH_DEBUG
  const size_t read = in.read_bytes(reinterpret_cast<byte_type*>(&str[0]), len);
  assert(read == len);
#else
  in.read_bytes(reinterpret_cast<byte_type*>(&str[0]), len);
#endif // IRESEARCH_DEBUG

  return str;
}

template< typename ContType >
inline ContType read_strings(data_input& in) {
  ContType c;

  const size_t size = read_size(in);
  c.reserve(size);

  for (size_t i = 0; i < size; ++i) {
    c.emplace(read_string< typename ContType::value_type >(in));
  }

  return c;
}

// ----------------------------------------------------------------------------
// --SECTION--                                                     skip helpers 
// ----------------------------------------------------------------------------

const uint64_t SKIP_BUFFER_SIZE = 1024U;

IRESEARCH_API void skip(
  data_input& in, size_t to_skip,
  byte_type* skip_buf, size_t skip_buf_size
);

// ----------------------------------------------------------------------------
// --SECTION--                                                  packing helpers 
// ----------------------------------------------------------------------------

IRESEARCH_API void read_packed(data_input& in, uint32_t bits, uint32_t* dst, uint32_t size);

IRESEARCH_API void read_packed(data_input& in, uint32_t* dst, uint32_t size);

IRESEARCH_API void write_packed(data_output& out, uint32_t bits,
                                const uint32_t* data, uint32_t size);

IRESEARCH_API void write_packed(data_output& out, const uint32_t* values, uint32_t count);

IRESEARCH_API void skip_block(index_input& in, uint32_t size,
                              uint32_t* encoded);

IRESEARCH_API void read_block(
  data_input& in, uint32_t size, uint32_t* encoded, uint32_t* decoded
);

IRESEARCH_API void read_block(
  data_input& in, uint32_t size, uint64_t* encoded, uint64_t* decoded
);

IRESEARCH_API void write_block(
  data_output& out, const uint32_t* decoded, uint32_t size, uint32_t* encoded
);

IRESEARCH_API void write_block(
  data_output& out, const uint64_t* decoded, uint32_t size, uint64_t* encoded
);

inline uint64_t shift_pack_64(uint64_t val, bool b) {
  assert(val <= 0x7FFFFFFFFFFFFFFFLL);
  return (val << 1) | (b ? 1 : 0);
}

inline uint32_t shift_pack_32(uint32_t val, bool b) {
  assert(val <= 0x7FFFFFFF);
  return (val << 1) | (b ? 1 : 0);
}

inline bool shift_unpack_64(uint64_t in, uint64_t& out) {
  out = in >> 1;
  return in & 1;
}

inline bool shift_unpack_32(uint32_t in, uint32_t& out) {
  out = in >> 1;
  return in & 1;
}

// ----------------------------------------------------------------------------
// --SECTION--                                                      I/O streams
// ----------------------------------------------------------------------------

class IRESEARCH_API bytes_output final: public data_output, public bytes_ref {
 public:
  DECLARE_PTR( bytes_output );

  bytes_output() = default;
  explicit bytes_output( size_t capacity );
  bytes_output( bytes_output&& rhs );
  bytes_output& operator=( bytes_output&& rhs );

  void reset() { this->size_ = 0; }

  virtual void write_byte( byte_type b ) override {
    oversize(buf_, this->size() + 1).replace(this->size(), 1, 1, b);
    this->data_ = buf_.data();
    ++this->size_;
  }

  virtual void write_bytes( const byte_type* b, size_t size ) override {
    oversize(buf_, this->size() + size).replace(this->size(), size, b, size);
    this->data_ = buf_.data();
    this->size_ += size;
  }

  virtual void close() override { }

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  bstring buf_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

class IRESEARCH_API bytes_ref_input final : public data_input{
 public:
  DECLARE_PTR(bytes_ref_input);

  bytes_ref_input();
  explicit bytes_ref_input(const bytes_ref& data);

  void skip(size_t size) {
    assert(pos_ + size < data_.size());
    pos_ += size;
  }

  void seek(size_t pos) {
    assert(pos < data_.size());
    pos_ = pos;
  }

  virtual size_t file_pointer() const override {
    return pos_;
  }

  virtual size_t length() const override {
    return data_.size();
  }
  
  virtual bool eof() const override {
    return pos_ >= data_.size();
  }

  virtual byte_type read_byte() override;
  virtual size_t read_bytes(byte_type* b, size_t size) override;
  void read_bytes(bstring& buf, size_t size); // append to buf

  void reset(const byte_type* data, size_t size) {
    data_ = bytes_ref(data, size);
    pos_ = 0;
  }

 private:
  bytes_ref data_;
  size_t pos_;
};

class IRESEARCH_API bytes_input final: public data_input, public bytes_ref {
 public:
  DECLARE_PTR(bytes_input);

  bytes_input();
  explicit bytes_input(const bytes_ref& data);
  bytes_input(bytes_input&& rhs);
  bytes_input& operator=(bytes_input&& rhs);
  bytes_input& operator=(const bytes_ref& data);

  void read_from(data_input& in, size_t size);

  void skip(size_t size) {
    assert(pos_ + size <= this->size());
    pos_ += size;
  }

  void seek(size_t pos) {
    assert(pos <= this->size());
    pos_ = pos;
  }

  virtual size_t file_pointer() const override { 
    return pos_; 
  }

  virtual size_t length() const override { 
    return this->size(); 
  }

  virtual bool eof() const override {
    return pos_ >= this->size();
  }

  virtual byte_type read_byte() override;
  virtual size_t read_bytes(byte_type* b, size_t size) override;
  void read_bytes(bstring& buf, size_t size); // append to buf

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  bstring buf_;
  size_t pos_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
};

NS_END

#endif