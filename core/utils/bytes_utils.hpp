////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#ifndef IRESEARCH_BYTES_UTILS_H
#define IRESEARCH_BYTES_UTILS_H

NS_ROOT

template<typename T, size_t N>
struct bytes_io;

template<typename T>
struct bytes_io<T, sizeof(uint32_t)> {
  template<typename OutputIterator>
  static void vwrite(OutputIterator& out, T in, std::output_iterator_tag) {
    while (in >= 0x80) {
      *out = static_cast<irs::byte_type>(in | 0x80); ++out;
      in >>= 7;
    }

    *out = static_cast<irs::byte_type>(in); ++out;
  }

  template<typename OutputIterator>
  static void write(OutputIterator& out, T in, std::output_iterator_tag) {
    *out = static_cast<irs::byte_type>(in >> 24); ++out;
    *out = static_cast<irs::byte_type>(in >> 16); ++out;
    *out = static_cast<irs::byte_type>(in >> 8);  ++out;
    *out = static_cast<irs::byte_type>(in);       ++out;
  }

  template<typename InputIterator>
  static T vread(InputIterator& in, std::input_iterator_tag) {
    T out = *in; ++in; if (!(out & 0x80)) return out;

    T b;
    out -= 0x80;
    b = *in; ++in; out += b << 7; if (!(b & 0x80)) return out;
    out -= 0x80 << 7;
    b = *in; ++in; out += b << 14; if (!(b & 0x80)) return out;
    out -= 0x80 << 14;
    b = *in; ++in; out += b << 21; if (!(b & 0x80)) return out;
    out -= 0x80 << 21;
    b = *in; ++in; out += b << 28;
    // last byte always has MSB == 0, so we don't need to check and subtract 0x80

    return out;
  }

  template<typename InputIterator>
  static T read(InputIterator& in, std::input_iterator_tag) {
    T out = static_cast<T>(*in) << 24; ++in;
    out |= static_cast<T>(*in) << 16;  ++in;
    out |= static_cast<T>(*in) << 8;   ++in;
    out |= static_cast<T>(*in);        ++in;

    return out;
  }
}; // bytes_io<T, sizeof(uint32_t)>

template<typename T>
struct bytes_io<T, sizeof(uint64_t)> {
  template<typename OutputIterator>
  static void vwrite(OutputIterator& out, T in, std::output_iterator_tag) {
    while (in >= T(0x80)) {
      *out = static_cast<irs::byte_type>(in | T(0x80)); ++out;
      in >>= 7;
    }

    *out = static_cast<irs::byte_type>(in); ++out;
  }

  template<typename OutputIterator>
  static void write(OutputIterator& out, T in, std::output_iterator_tag) {
    typedef bytes_io<uint32_t, sizeof(uint32_t)> bytes_io_t;

    bytes_io_t::write(out, static_cast<uint32_t>(in >> 32), std::output_iterator_tag{});
    bytes_io_t::write(out, static_cast<uint32_t>(in), std::output_iterator_tag{});
  }

  template<typename InputIterator>
  static T vread(InputIterator& in, std::input_iterator_tag) {
    const T MASK = 0x80;
    T out = *in; ++in; if (!(out & MASK)) return out;

    T b;
    out -= MASK;
    b = *in; ++in; out += b << 7; if (!(b & MASK)) return out;
    out -= MASK << 7;
    b = *in; ++in; out += b << 14; if (!(b & MASK)) return out;
    out -= MASK << 14;
    b = *in; ++in; out += b << 21; if (!(b & MASK)) return out;
    out -= MASK << 21;
    b = *in; ++in; out += b << 28; if (!(b & MASK)) return out;
    out -= MASK << 28;
    b = *in; ++in; out += b << 35; if (!(b & MASK)) return out;
    out -= MASK << 35;
    b = *in; ++in; out += b << 42; if (!(b & MASK)) return out;
    out -= MASK << 42;
    b = *in; ++in; out += b << 49; if (!(b & MASK)) return out;
    out -= MASK << 49;
    b = *in; ++in; out += b << 56; if (!(b & MASK)) return out;
    out -= MASK << 56;
    b = *in; ++in; out += b << 63;
    // last byte always has MSB == 0, so we don't need to check and subtract 0x80

    return out;
  }

  template<typename InputIterator>
  static T read(InputIterator& in, std::input_iterator_tag) {
    typedef bytes_io<uint32_t, sizeof(uint32_t)> bytes_io_t;

    T out = static_cast<T>(bytes_io_t::read(in, std::input_iterator_tag{})) << 32;
    return out | static_cast<T>(bytes_io_t::read(in, std::input_iterator_tag{}));
  }
}; // bytes_io<T, sizeof(uint64_t)>

template<typename T, typename Iterator>
inline void write(Iterator& out, T in) {
  bytes_io<T, sizeof(T)>::write(out, in, typename std::iterator_traits<Iterator>::iterator_category());
}

template<typename T, typename Iterator>
inline void vwrite(Iterator& out, T in) {
  bytes_io<T, sizeof(T)>::vwrite(out, in, typename std::iterator_traits<Iterator>::iterator_category());
}

template<typename T, typename Iterator>
inline T read(Iterator& in) {
  return bytes_io<T, sizeof(T)>::read(in, typename std::iterator_traits<Iterator>::iterator_category());
}

template<typename T, typename Iterator>
inline T vread(Iterator& in) {
  return bytes_io<T, sizeof(T)>::vread(in, typename std::iterator_traits<Iterator>::iterator_category());
}

NS_END

#endif // IRESEARCH_BYTES_UTILS_H
