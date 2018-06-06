////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////


#ifdef _WIN32
  #include <Windows.h> // for GetACP()
#else
  #include <langinfo.h> // for nl_langinfo(...)
#endif

#include <algorithm>
#include <cstring>
#include <map>
#include <unordered_map>

#if defined (__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

  #include <boost/locale/generator.hpp>

#if defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

#include <boost/locale/info.hpp>

#if defined (__GNUC__)
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

  #include <boost/locale/util.hpp>

#if defined (__GNUC__)
  #pragma GCC diagnostic pop
#endif

#include <unicode/coll.h> // for icu::Collator
#include <unicode/decimfmt.h> // for icu::DecimalFormat
#include <unicode/numfmt.h> // for icu::NumberFormat
#include <unicode/ucnv.h> // for UConverter

#include "hash_utils.hpp"
#include "map_utils.hpp"
#include "object_pool.hpp"
#include "numeric_utils.hpp"
#include "error/error.hpp"

#include "locale_utils.hpp"

NS_LOCAL

////////////////////////////////////////////////////////////////////////////////
/// @brief size of internal buffers, arbitrary size
////////////////////////////////////////////////////////////////////////////////
const size_t BUFFER_SIZE = 1024;

////////////////////////////////////////////////////////////////////////////////
/// @brief size of ICU object pools, arbitrary size
////////////////////////////////////////////////////////////////////////////////
const size_t POOL_SIZE = 8;

// -----------------------------------------------------------------------------
// --SECTION--                                    facets required by std::locale
// -----------------------------------------------------------------------------

std::string system_encoding() {
  #ifdef _WIN32
    static std::string prefix("cp");

    return prefix + to_string(GetACP());
  #else
    return nl_langinfo(CODESET);
  #endif
}

////////////////////////////////////////////////////////////////////////////////
/// @brief a thread-safe pool of ICU converters for a given encoding
///        may hold nullptr on ICU converter instantiation failure
////////////////////////////////////////////////////////////////////////////////
class converter_pool: private irs::util::noncopyable {
 public:
  DECLARE_SHARED_PTR(UConverter);
  converter_pool(std::string&& encoding)
    : encoding_(std::move(encoding)), pool_(POOL_SIZE) {}
  ptr get() { return pool_.emplace(encoding_).release(); }
  const std::string& encoding() const NOEXCEPT { return encoding_; }

 private:
  struct builder {
    DECLARE_SHARED_PTR(UConverter);
    static ptr make(const std::string& encoding) {
      UErrorCode status = U_ZERO_ERROR;
      ptr value(
        ucnv_open(encoding.c_str(), &status),
        [](UConverter* ptr)->void{ ucnv_close(ptr); }
      );

      return U_SUCCESS(status) ? std::move(value) : nullptr;
    }
  };

  std::string encoding_;
  irs::unbounded_object_pool<builder> pool_;
};

////////////////////////////////////////////////////////////////////////////////
/// @param encoding the converter encoding (null == system encoding)
/// @@return a converter for the specified encoding
////////////////////////////////////////////////////////////////////////////////
converter_pool& get_converter(const irs::string_ref& encoding) {
  static auto generator = [](
      const irs::hashed_string_ref& key,
      const converter_pool& pool
  ) NOEXCEPT->irs::hashed_string_ref {
    // reuse hash but point ref at value in pool
    return irs::hashed_string_ref(key.hash(), pool.encoding());
  };
  static std::mutex mutex;
  static std::unordered_map<irs::hashed_string_ref, converter_pool> encodings;
  auto key = encoding;
  std::string tmp;

  // use system encoding if encoding.null()
  if (key.null()) {
    tmp = system_encoding();
    key = tmp;
  }

  SCOPED_LOCK(mutex);

  return irs::map_utils::try_emplace_update_key(
    encodings,
    generator,
    irs::make_hashed_ref(key, std::hash<irs::string_ref>()),
    key
  ).first->second;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief base implementation for converters between 'internal' representation
///        and an 'external' user-specified encoding
////////////////////////////////////////////////////////////////////////////////
template<typename InternType>
class codecvt_base: public std::codecvt<InternType, char, mbstate_t> {
 public:
  typedef std::codecvt<InternType, char, mbstate_t> parent_t;
  typedef typename parent_t::extern_type extern_type;
  typedef typename parent_t::intern_type intern_type;
  typedef typename parent_t::state_type state_type;

  codecvt_base(converter_pool& converters): converters_(converters) {}

 protected:
  struct context_t {
    DECLARE_UNIQUE_PTR(context_t);
    std::basic_string<typename parent_t::extern_type> buf_ext_;
    std::basic_string<typename parent_t::intern_type> buf_int_;
    converter_pool::ptr converter_;

    static ptr make(converter_pool& pool) {
      auto ctx = irs::memory::make_unique<context_t>();

      ctx->converter_ = pool.get();

      return ctx->converter_ ? std::move(ctx) : nullptr;
    }
  };
  typedef irs::unbounded_object_pool<context_t> context_pool;

  typename context_pool::ptr context() const {
    return contexts_.emplace(converters_);
  }

  const std::string& context_encoding() const { return converters_.encoding(); }

  virtual bool do_always_noconv() const NOEXCEPT final override {
    return false; // not an identity conversion
  }

  virtual int do_encoding() const NOEXCEPT override = 0;
  virtual std::codecvt_base::result do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
  ) const override = 0;
  virtual int do_length(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    std::size_t max
  ) const final override;
  virtual int do_max_length() const NOEXCEPT override = 0;
  virtual std::codecvt_base::result do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override = 0;
  virtual std::codecvt_base::result do_unshift(
    state_type& state,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const final override;

 private:
  mutable context_pool contexts_;
  converter_pool& converters_;
};

template<typename InternType>
int codecvt_base<InternType>::do_length(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    std::size_t max
) const {
  auto ctx = context();

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while computing number of required input characters from encoding '%s' to produce at most '" IR_SIZE_T_SPECIFIER "' output characters",
      context_encoding().c_str(), max
    );

    return std::codecvt_base::error;
  }

  ctx->buf_int_.resize(max);

  auto* from_next = from;
  auto* to = &(ctx->buf_int_[0]);
  auto* to_end = to + max;
  auto* to_next = to;
  auto res = do_in(state, from, from_end, from_next, to, to_end, to_next);

  return res == std::codecvt_base::ok ? std::distance(from, from_next) : 0;
}

template<typename InternType>
std::codecvt_base::result codecvt_base<InternType>::do_unshift(
    state_type& state,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
) const {
  to_next = to;

  return std::codecvt_base::ok;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief converter between an 'internal' utf16 representation and
///        an 'external' user-specified encoding
////////////////////////////////////////////////////////////////////////////////
class codecvt16_facet final: public codecvt_base<char16_t> {
 public:
  codecvt16_facet(converter_pool& converters): codecvt_base(converters) {}

 protected:
  virtual int do_encoding() const NOEXCEPT override;
  virtual std::codecvt_base::result do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
  ) const override;
  virtual int do_max_length() const NOEXCEPT override;
  virtual std::codecvt_base::result do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override;
};

int codecvt16_facet::do_encoding() const NOEXCEPT {
  auto ctx = context();

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while computing number of required input characters from encoding '%s' to produce a single output character",
      context_encoding().c_str()
    );

    return -1;
  }

  UErrorCode status = U_ZERO_ERROR;

  // the exact number of externT characters that correspond to one internT character, if constant
  return ucnv_isFixedWidth(ctx->converter_.get(), &status)
    ? ucnv_getMinCharSize(ctx->converter_.get()) : 0;
}

std::codecvt_base::result codecvt16_facet::do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
) const {
  auto ctx = context();

  from_next = from;
  to_next = to;

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while converting encoding '%s' to unicode system encoding",
      context_encoding().c_str()
    );

    return std::codecvt_base::error;
  }

  UErrorCode status = U_ZERO_ERROR;

  static_assert(sizeof(UChar) == sizeof(intern_type), "sizeof(UChar) != sizeof(intern_type)");
  ucnv_toUnicode(
    ctx->converter_.get(),
    reinterpret_cast<UChar**>(&to_next),
    reinterpret_cast<const UChar*>(to_end),
    &from_next,
    from_end,
    nullptr,
    true,
    &status
  );

  if (U_BUFFER_OVERFLOW_ERROR == status) {
    return std::codecvt_base::partial; // destination buffer is not large enough
  }

  if (!U_SUCCESS(status)) {
    from_next = from;
    to_next = to;

    IR_FRMT_WARN(
      "failure to convert from locale encoding to UTF16 while converting encoding '%s' unicode system encoding",
      context_encoding().c_str()
    );

    return std::codecvt_base::error; // error occured during final conversion
  }

  return std::codecvt_base::ok;
}

int codecvt16_facet::do_max_length() const NOEXCEPT {
  auto ctx = context();

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while computing maximum number of required input characters from encoding '%s' to produce a single output character",
      context_encoding().c_str()
    );

    return -1;
  }

  return ucnv_getMaxCharSize(ctx->converter_.get());
}

std::codecvt_base::result codecvt16_facet::do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
) const {
  auto ctx = context();

  from_next = from;
  to_next = to;

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while converting unicode system encoding to encoding '%s'",
      context_encoding().c_str()
    );

    return std::codecvt_base::error;
  }

  UErrorCode status = U_ZERO_ERROR;

  static_assert(sizeof(UChar) == sizeof(intern_type), "sizeof(UChar) != sizeof(intern_type)");
  ucnv_fromUnicode(
    ctx->converter_.get(),
    &to_next,
    to_end,
    reinterpret_cast<const UChar**>(&from_next),
    reinterpret_cast<const UChar *>(from_end),
    nullptr,
    true,
    &status
  );

  if (U_BUFFER_OVERFLOW_ERROR == status) {
    return std::codecvt_base::partial; // destination buffer is not large enough
  }

  if (!U_SUCCESS(status)) {
    from_next = from;
    to_next = to;

    IR_FRMT_WARN(
      "failure to convert from TF16 to locale encoding while converting unicode system encoding to encoding '%s'",
      context_encoding().c_str()
    );

    return std::codecvt_base::error; // error occured during final conversion
  }

  return std::codecvt_base::ok;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief converter between an 'internal' utf32 representation and
///        an 'external' user-specified encoding and an
////////////////////////////////////////////////////////////////////////////////
class codecvt32_facet final: public codecvt_base<char32_t> {
 public:
  codecvt32_facet(converter_pool& converters): codecvt_base(converters) {}

 protected:
  virtual int do_encoding() const NOEXCEPT final override;
  virtual std::codecvt_base::result do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
  ) const final override;
  virtual int do_max_length() const NOEXCEPT override;
  virtual std::codecvt_base::result do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override;
};

int codecvt32_facet::do_encoding() const NOEXCEPT {
  auto ctx = context();

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while computing number of required input characters from encoding '%s' to produce a single output character",
      context_encoding().c_str()
    );

    return -1;
  }

  UErrorCode status = U_ZERO_ERROR;

  // the exact number of extern_type characters that correspond to one intern_type character, if constant
  return ucnv_isFixedWidth(ctx->converter_.get(), &status)
    ? ucnv_getMinCharSize(ctx->converter_.get()) : 0;
}

std::codecvt_base::result codecvt32_facet::do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
) const {
  auto ctx = context();

  from_next = from;
  to_next = to;

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while converting encoding '%s' to unicode system encoding",
      context_encoding().c_str()
    );

    return std::codecvt_base::error;
  }

  UChar buf[BUFFER_SIZE];
  auto* buf_end = buf + IRESEARCH_COUNTOF(buf);
  int32_t offsets[IRESEARCH_COUNTOF(buf) + 1]; // +1 for end

  ucnv_reset(ctx->converter_.get());

  // convert 'BUFFER_SIZE' at a time
  while (from_next < from_end) {
    auto* buf_next = buf;
    auto* from_next_prev = from_next;
    auto* to_next_prev = to_next;
    UErrorCode src_status = U_ZERO_ERROR;
    UErrorCode dst_status = U_ZERO_ERROR;

    // convert from desired encoding to the intermediary representation
    ucnv_toUnicode(
      ctx->converter_.get(),
      &buf_next,
      buf_end,
      &from_next,
      from_end,
      offsets,
      true,
      &src_status
    );

    if (!U_SUCCESS(src_status) && U_BUFFER_OVERFLOW_ERROR != src_status) {
      from_next = from_next_prev;
      to_next = to_next_prev;

      IR_FRMT_WARN(
        "failure to convert from locale encoding to UTF16 while converting encoding '%s' unicode system encoding",
        context_encoding().c_str()
      );

      return std::codecvt_base::error; // error occured during final conversion
    }

    assert(buf_next >= buf && IRESEARCH_COUNTOF(buf) >= size_t(buf_next - buf)); // >= to account for past-end
    offsets[buf_next - buf] = from_next - from_next_prev; // remember past-end position

    int32_t to_used = 0;

    static_assert(sizeof(UChar32) == sizeof(intern_type), "sizeof(UChar32) != sizeof(intern_type)");
    u_strToUTF32(
      reinterpret_cast<UChar32*>(to_next),
      to_end - to_next,
      &to_used, // set to the number of output units corresponding to the transformation of all the input units, even in case of a buffer overflow
      buf,
      buf_next - buf,
      &dst_status
    );

    if ((U_SUCCESS(dst_status) || U_BUFFER_OVERFLOW_ERROR == dst_status)
        && to_used < 0) {
      dst_status = U_INTERNAL_PROGRAM_ERROR; // ICU internal error
    }

    if (!U_SUCCESS(dst_status) && U_BUFFER_OVERFLOW_ERROR != dst_status) {
      from_next = from_next_prev;
      to_next = to_next_prev;

      IR_FRMT_WARN(
        "failure to convert from UTF16 to UTF32 while converting encoding '%s' to unicode system encoding",
        context_encoding().c_str()
      );

      return std::codecvt_base::error; // error occured during final conversion
    }

    from_next =
      from_next_prev + offsets[std::min(IRESEARCH_COUNTOF(buf) + 1, size_t(to_used))]; // update successfully converted, +1 for end

    if (U_BUFFER_OVERFLOW_ERROR == dst_status
        || (U_BUFFER_OVERFLOW_ERROR == src_status && from_next >= from_end)) {
      return std::codecvt_base::partial; // destination buffer is not large enough
    }
  }

  return std::codecvt_base::ok;
}

int codecvt32_facet::do_max_length() const NOEXCEPT {
  auto ctx = context();

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while computing maximum number of required input characters from encoding '%s' to produce a single output character",
      context_encoding().c_str()
    );

    return -1;
  }

  return ucnv_getMaxCharSize(ctx->converter_.get()) * 2; // *2 for UTF16->UTF32 conversion
}

std::codecvt_base::result codecvt32_facet::do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
) const {
  auto ctx = context();

  from_next = from;
  to_next = to;

  if (!ctx) {
    IR_FRMT_WARN(
      "failure to get conversion context while converting unicode system encoding to encoding '%s'",
      context_encoding().c_str()
    );

    return std::codecvt_base::error;
  }

  UChar buf[BUFFER_SIZE];
  auto* buf_end = buf + IRESEARCH_COUNTOF(buf);
  size_t offsets[IRESEARCH_COUNTOF(buf) + 1]; // +1 for end

  ucnv_reset(ctx->converter_.get());

  // convert 'BUFFER_SIZE' at a time
  while (from_next < from_end) {
    const UChar* buf_from = buf;
    auto* buf_next = buf;
    auto* from_next_prev = from_next;
    auto* to_next_prev = to_next;
    UErrorCode src_status = U_ZERO_ERROR;
    UErrorCode dst_status = U_ZERO_ERROR;

    // convert one char at a time to track source position to destination position
    do {
      int32_t buf_size = buf_end - buf_next;
      int32_t buf_used = 0;

      static_assert(sizeof(UChar32) == sizeof(intern_type), "sizeof(UChar32) != sizeof(intern_type)");
      u_strFromUTF32(
        buf_next,
        buf_size,
        &buf_used, // set to the number of output units corresponding to the transformation of all the input units, even in case of a buffer overflow
        reinterpret_cast<const UChar32*>(from_next),
        1, // 1 char at a time to track source/destination position mapping
        &src_status
      );

      if (U_BUFFER_OVERFLOW_ERROR == src_status) {
        break; // conversion buffer not large enough to hold result
      }

      if (U_SUCCESS(src_status) && buf_used < 0) {
        src_status = U_INTERNAL_PROGRAM_ERROR; // ICU internal error
      }

      if (!U_SUCCESS(src_status)) {
        IR_FRMT_WARN(
          "failure to convert from UTF32 to UTF16 while converting unicode system encoding to encoding '%s'",
          context_encoding().c_str()
        );

        break; // finish copying all successfully converted
      }

      assert(buf_next >= buf && IRESEARCH_COUNTOF(buf) > size_t(buf_next - buf));
      offsets[buf_next - buf] = from_next - from; // remember converted position
      buf_next += buf_used;
      ++from_next; // +1 for 1 char at a time
    } while (from_next < from_end);

    assert(buf_next >= buf && IRESEARCH_COUNTOF(buf) >= size_t(buf_next - buf)); // >= to account for past-end
    offsets[buf_next - buf] = from_next - from; // remember past-end position

    // convert intermediary representation to the desired encoding
    ucnv_fromUnicode(
      ctx->converter_.get(),
      &to_next,
      to_end,
      &buf_from,
      buf_next,
      nullptr,
      true,
      &dst_status
    );

    if (!U_SUCCESS(dst_status) && U_BUFFER_OVERFLOW_ERROR != dst_status) {
      from_next = from_next_prev;
      to_next = to_next_prev;

      IR_FRMT_WARN(
        "failure to convert from UTF16 to locale encoding while converting unicode system encoding to encoding '%s'",
        context_encoding().c_str()
      );

      return std::codecvt_base::error; // error occured during final conversion
    }

    assert(buf_from >= buf && IRESEARCH_COUNTOF(buf) >= size_t(buf_from - buf));
    from_next = from + offsets[buf_from - buf]; // update successfully converted

    if (!U_SUCCESS(src_status) && U_BUFFER_OVERFLOW_ERROR != src_status) {
      return std::codecvt_base::error; // error occured during intermediary conversion
    }

    if (U_BUFFER_OVERFLOW_ERROR == dst_status
        || (U_BUFFER_OVERFLOW_ERROR == src_status && from_next >= from_end)) {
      return std::codecvt_base::partial; // destination buffer is not large enough
    }
  }

  return std::codecvt_base::ok;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief converter between an 'internal' utf8 representation and
///        an 'external' user-specified encoding and an
////////////////////////////////////////////////////////////////////////////////
class codecvt8u_facet: public std::codecvt<char, char, mbstate_t> {
 public:
  codecvt8u_facet(converter_pool& pool): pool_(pool) {}

 protected:
  virtual bool do_always_noconv() const NOEXCEPT override;
  virtual int do_encoding() const NOEXCEPT override;
  virtual std::codecvt_base::result do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
  ) const override;
  virtual int do_length(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    std::size_t max
  ) const override;
  virtual int do_max_length() const NOEXCEPT override;
  virtual std::codecvt_base::result do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override;
  virtual std::codecvt_base::result do_unshift(
    state_type& state,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override;

 private:
  converter_pool& pool_;
};

////////////////////////////////////////////////////////////////////////////////
/// @brief converter between an 'internal' utf8/utf16/uf32 representation,
///        based on sizeof(wchar_t), and
///        an 'external' user-specified encoding
////////////////////////////////////////////////////////////////////////////////
//fixme needs to be a template based on sizeof wchar_t
class codecvtwu_facet: public std::codecvt<wchar_t, char, mbstate_t> {
 public:
  codecvtwu_facet(converter_pool& pool): impl_(pool) {}

 protected:
  virtual bool do_always_noconv() const NOEXCEPT override {
    return impl_.always_noconv();
  }

  virtual int do_encoding() const NOEXCEPT override {
    return impl_.encoding();
  }

  virtual std::codecvt_base::result do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
  ) const override {
    static_assert(sizeof(impl_t::intern_type) == sizeof(intern_type), "sizeof(impl_t::intern_type) != sizeof(intern_type)");
    return impl_.in(
      state,
      from,
      from_end,
      from_next,
      reinterpret_cast<impl_t::intern_type*>(to),
      reinterpret_cast<impl_t::intern_type*>(to_end),
      reinterpret_cast<impl_t::intern_type*&>(to_next)
    );
  }

  virtual int do_length(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    std::size_t max
  ) const override {
    return impl_.length(state, from, from_end, max);
  }

  virtual int do_max_length() const NOEXCEPT override {
    return impl_.max_length();
  }

  virtual std::codecvt_base::result do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override {
    static_assert(sizeof(impl_t::intern_type) == sizeof(intern_type), "sizeof(impl_t::intern_type) != sizeof(intern_type)");
    return impl_.out(
      state,
      reinterpret_cast<const impl_t::intern_type*>(from),
      reinterpret_cast<const impl_t::intern_type*>(from_end),
      reinterpret_cast<const impl_t::intern_type*&>(from_next),
      to,
      to_end,
      to_next
    );
  }

  virtual std::codecvt_base::result do_unshift(
    state_type& state,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override {
    return impl_.unshift(state, to, to_end, to_next);
  }

 private:
  typedef std::conditional<
    sizeof(char32_t) == sizeof(wchar_t),
    codecvt32_facet,
    std::conditional<
      sizeof(char16_t) == sizeof(wchar_t),
      codecvt16_facet,
      std::conditional<
        sizeof(char) == sizeof(wchar_t),
        codecvt8u_facet,
        void
      >::type
    >::type
  >::type impl_t; // unicode implementation depends on sizeof(whcar_t)

  impl_t impl_;
};

////////////////////////////////////////////////////////////////////////////////
/// @brief converter between an 'internal' 'system' encoding representation and
///        an 'external' user-specified encoding and an
////////////////////////////////////////////////////////////////////////////////
class codecvt8_facet: public std::codecvt<char, char, mbstate_t> {
 public:
  codecvt8_facet(converter_pool& pool): pool_(pool) {}

 protected:
  virtual bool do_always_noconv() const NOEXCEPT override;
  virtual int do_encoding() const NOEXCEPT override;
  virtual std::codecvt_base::result do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
  ) const override;
  virtual int do_length(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    std::size_t max
  ) const override;
  virtual int do_max_length() const NOEXCEPT override;
  virtual std::codecvt_base::result do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override;
  virtual std::codecvt_base::result do_unshift(
    state_type& state,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override;

 private:
  converter_pool& pool_;
};

////////////////////////////////////////////////////////////////////////////////
/// @brief converter between an 'internal' 'system' encoding representation and
///        an 'external' user-specified encoding and an
////////////////////////////////////////////////////////////////////////////////
class codecvtw_facet: public std::codecvt<wchar_t, char, mbstate_t> {
 public:
  codecvtw_facet(converter_pool& pool): pool_(pool) {}

 protected:
  virtual bool do_always_noconv() const NOEXCEPT override;
  virtual int do_encoding() const NOEXCEPT override;
  virtual std::codecvt_base::result do_in(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    const extern_type*& from_next,
    intern_type* to,
    intern_type* to_end,
    intern_type*& to_next
  ) const override;
  virtual int do_length(
    state_type& state,
    const extern_type* from,
    const extern_type* from_end,
    std::size_t max
  ) const override;
  virtual int do_max_length() const NOEXCEPT override;
  virtual std::codecvt_base::result do_out(
    state_type& state,
    const intern_type* from,
    const intern_type* from_end,
    const intern_type*& from_next,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override;
  virtual std::codecvt_base::result do_unshift(
    state_type& state,
    extern_type* to,
    extern_type* to_end,
    extern_type*& to_next
  ) const override;

 private:
  converter_pool& pool_;
};

template<typename CharType>
class codecvt_facet: public std::codecvt<char, CharType, mbstate_t> {
 public:
  typedef typename std::codecvt<char, CharType, mbstate_t>::extern_type extern_type;

  codecvt_facet(const irs::string_ref& encoding)
    : encoding_(encoding), pool_(POOL_SIZE) {
  }

  // FIXME TODO implement

  bool out(icu::UnicodeString& buf, icu::StringPiece) const {
    // FIXME TODO implement
    return false;
  }

  bool out(
    std::basic_string<extern_type>& buf, const icu::UnicodeString& value
  ) const;

 private:
  // 'make(...)' method wrapper for icu::UConverter
  struct builder {
    DECLARE_SHARED_PTR(UConverter);
    static ptr make(const std::string& encoding) {
      UErrorCode status = U_ZERO_ERROR;
      ptr value(
        ucnv_open(encoding.c_str(), &status),
        [](UConverter* ptr)->void{ ucnv_close(ptr); }
      );

      return U_SUCCESS(status) ? std::move(value) : nullptr;
    }
  };

  std::string encoding_;
  mutable irs::unbounded_object_pool<builder> pool_;
};

template<typename CharType>
bool codecvt_facet<CharType>::out(
    std::basic_string<extern_type>& buf, const icu::UnicodeString& value
) const {
  char ch_buf[1024];
  auto conv = pool_.emplace(encoding_);

  if (!conv) {
    return false;
  }

  auto* dst_end = ch_buf + IRESEARCH_COUNTOF(ch_buf);
  auto* src_begin = value.getBuffer();
  auto* src_end = src_begin + value.length();
  UErrorCode status = U_ZERO_ERROR;

  ucnv_reset(conv.get());

  do {
    auto* dst_begin = ch_buf;

    ucnv_fromUnicode(
      conv.get(),
      &dst_begin, dst_end,
      &src_begin, src_end,
      nullptr,
      true,
      &status
    );
    buf.append(ch_buf, dst_begin - ch_buf);

    if (U_SUCCESS(status)) {
      return true;
    }
  } while (status == U_BUFFER_OVERFLOW_ERROR);

  return false;
}

class collate_facet: public std::collate<char> {
  // FIXME TODO implement
};

class collatew_facet: public std::collate<wchar_t> {
  // FIXME TODO implement
};

class ctype_facet: public std::ctype<char> {
  // FIXME TODO implement
};

class ctypew_facet: public std::ctype<wchar_t> {
  // FIXME TODO implement
};

class money_get_facet: public std::money_get<char> {
  // FIXME TODO implement
};

class money_getw_facet: public std::money_get<wchar_t> {
  // FIXME TODO implement
};

class money_put_facet: public std::money_put<char> {
  // FIXME TODO implement
};

class money_putw_facet: public std::money_put<wchar_t> {
  // FIXME TODO implement
};

class moneypunct_facet: public std::moneypunct<char> {
  // FIXME TODO implement
};

class moneypunctintl_facet: public std::moneypunct<char, true> {
  // FIXME TODO implement
};

class moneypunctw_facet: public std::moneypunct<wchar_t> {
  // FIXME TODO implement
};

class moneypunctwintl_facet: public std::moneypunct<wchar_t, true> {
  // FIXME TODO implement
};

class num_get_facet: public std::num_get<char> {
  // FIXME TODO implement
};

class num_getw_facet: public std::num_get<wchar_t> {
  // FIXME TODO implement
};

template<typename CharType>
class num_put_facet: public std::num_put<CharType> {
 public:
  typedef typename std::num_put<CharType>::char_type char_type;
  typedef typename std::num_put<CharType>::iter_type iter_type;

  num_put_facet(const icu::Locale& locale, const codecvt_facet<char_type>& utf8)
    : locale_(locale), pool_(POOL_SIZE), utf8_(utf8) {
  }

 protected:
  virtual iter_type do_put(
    iter_type out, std::ios_base& str, char_type fill, bool value
  ) const override;
  virtual iter_type do_put(
    iter_type out, std::ios_base& str, char_type fill, long value
  ) const override;
  virtual iter_type do_put(
    iter_type out, std::ios_base& str, char_type fill, long long value
  ) const override;
  virtual iter_type do_put(
    iter_type out, std::ios_base& str, char_type fill, unsigned long value
  ) const override;
  virtual iter_type do_put(
    iter_type out, std::ios_base& str, char_type fill, unsigned long long value
  ) const override;
  virtual iter_type do_put(
    iter_type out, std::ios_base& str, char_type fill, double value
  ) const override;
  virtual iter_type do_put(
    iter_type out, std::ios_base& str, char_type fill, long double value
  ) const override;
  virtual iter_type do_put(
      iter_type out, std::ios_base& str, char_type fill, const void* value
  ) const override;

 private:
  struct context {
    DECLARE_UNIQUE_PTR(context);
    std::basic_string<char_type> buf_;
    UnicodeString icu_buf0_;
    UnicodeString icu_buf1_;
    std::unique_ptr<icu::NumberFormat> regular_;
    std::unique_ptr<icu::NumberFormat> scientific_; // uppercase (instead of mixed case by default)

    static ptr make(const icu::Locale& locale) {
      auto ctx = irs::memory::make_unique<context>();

      if (!ctx) {
        return nullptr;
      }

      UErrorCode status = U_ZERO_ERROR;

      ctx->regular_.reset(icu::NumberFormat::createInstance(locale, status));

      if (!U_SUCCESS(status) && !ctx->regular_) {
        return nullptr;
      }

      // at least on ICU v55/v57/v59 createScientificInstance(...) will create different,
      // (even on the same version but different hosts) and mostly incorrct formats,
      // e.g. incorrect decimal or exponent precision
      // hence use createInstance(...) and cast to DecimalFormat as per ICU documentation
      ctx->scientific_.reset(icu::NumberFormat::createInstance(locale, status));

      if (!U_SUCCESS(status) && !ctx->scientific_) {
        return nullptr;
      }

      auto* decimal = dynamic_cast<icu::DecimalFormat*>(ctx->scientific_.get());

      if (!decimal) {
        return nullptr; // can't set to scientific
      }

      decimal->setScientificNotation(true);

      // uppercase (instead of mixed case with UDisplayContext::UDISPCTX_CAPITALIZATION_NONE)
      ctx->scientific_->setContext(UDisplayContext::UDISPCTX_CAPITALIZATION_FOR_STANDALONE, status);

      if (!U_SUCCESS(status)) {
        return nullptr;
      }

      return std::move(ctx);
    }

    void reset(const std::ios_base& str) {
      auto grouping =
        !std::use_facet<std::numpunct<char_type>>(str.getloc()).grouping().empty();

      buf_.clear();
      icu_buf0_.truncate(0);
      icu_buf1_.truncate(0);
      regular_->setGroupingUsed(grouping);
      regular_->setMinimumFractionDigits(0);
      regular_->setMaximumFractionDigits(0);
      scientific_->setGroupingUsed(grouping);
      scientific_->setMinimumFractionDigits(0);
      scientific_->setMaximumFractionDigits(0);
    }
  };

  icu::Locale locale_;
  mutable irs::unbounded_object_pool<context> pool_;
  const codecvt_facet<char_type>& utf8_; // conversion from char->utf8

  template<typename T>
  static iter_type do_put_float_hex(
      iter_type out, std::ios_base& str, char_type fill, T value
  );

  template<typename T>
  static iter_type do_put_int_hex(
      iter_type out, std::ios_base& str, char_type fill, T value, bool full_width
  );

  template<typename T>
  static iter_type do_put_int_oct(
      iter_type out, std::ios_base& str, char_type fill, T value
  );

  static iter_type do_put_int_zero(
      iter_type out, std::ios_base& str, char_type fill
  );
};

template<typename CharType>
typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put(
    iter_type out, std::ios_base& str, char_type fill, bool value
) const {
  if (!(str.flags() & std::ios_base::boolalpha)) {
    return do_put(out, str, fill, long(value));
  }

  auto val = value
    ? std::use_facet<std::numpunct<char_type>>(str.getloc()).truename()
    : std::use_facet<std::numpunct<char_type>>(str.getloc()).falsename()
    ;
  auto rpad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::left
            ? str.width() : size_t(0)
            ;
  auto lpad = !rpad ? str.width() : size_t(0);
  size_t size = 0;

  str.width(0); // reset padding

  for (size_t i = lpad < val.size() ? 0 : lpad - val.size(); i; --i) {
    *out++ = fill;
    ++size;
  }

  for (size_t i = 0, count = val.size(); i < count; ++i) {
    *out++ = val[i];
    ++size;
  }

  for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
    *out++ = fill;
  }

  return out;
}

template<typename CharType>
typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put(
    iter_type out, std::ios_base& str, char_type fill, long value
) const {
  if (str.flags() & std::ios_base::oct) {
    return do_put_int_oct(out, str, fill, (unsigned long)value);
  }

  if (str.flags() & std::ios_base::hex) {
    return do_put_int_hex(out, str, fill, (unsigned long)value, false);
  }

  // the ICU operations are identical
  return do_put(out, str, fill, (long long)value);
}

template<typename CharType>
typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put(
    iter_type out, std::ios_base& str, char_type fill, long long value
) const {
  if (str.flags() & std::ios_base::oct) {
    return do_put_int_oct(out, str, fill, (unsigned long long)value);
  }

  if (str.flags() & std::ios_base::hex) {
    static_assert(sizeof(uint64_t) == sizeof(unsigned long long), "sizeof(uint64_t) != sizeof(unsigned long long)");
    return do_put_int_hex(out, str, fill, (uint64_t)value, false);
  }

  if (value >= 0) {
    return do_put(out, str, fill, (unsigned long long)value);
  }

  auto ipad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::internal
            ? str.width() : size_t(0)
            ;
  auto rpad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::left
            ? str.width() : size_t(0)
            ;
  auto lpad = !ipad && !rpad ? str.width() : size_t(0);
  size_t size = 0;

  str.width(0); // reset padding

  auto ctx = pool_.emplace(locale_);

  if (!ctx) {
    throw irs::detailed_io_error("failed to retrieve ICU formatter in num_put_facet::do_put(...)");
  }

  static_assert(sizeof(int64_t) == sizeof(long long), "sizeof(int64_t) != sizeof(long long)");
  ctx->reset(str);
  ctx->regular_->format(int64_t(0 - value), ctx->icu_buf0_);

  if (!utf8_.out(ctx->buf_, ctx->icu_buf0_)) {
    throw irs::detailed_io_error("failed to convert data from UTF8 in num_put_facet::do_put(...)");
  }

  size_t len = ctx->buf_.size() + 1; // +1 for '-'

  for (size_t i = lpad < len ? 0 : lpad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  *out++ = '-';
  ++size;

  for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  for (size_t i = 0, count = ctx->buf_.size(); i < count; ++i) {
    *out++ = ctx->buf_[i];
    ++size;
  }

  for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
    *out++ = fill;
  }

  return out;
}

template<typename CharType>
typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put(
    iter_type out, std::ios_base& str, char_type fill, unsigned long value
) const {
  if (str.flags() & std::ios_base::oct) {
    return do_put_int_oct(out, str, fill, (unsigned long)value);
  }

  if (str.flags() & std::ios_base::hex) {
    return do_put_int_hex(out, str, fill, (unsigned long)value, false);
  }

  // the ICU operations are identical
  return do_put(out, str, fill, (unsigned long long)value);
}

template<typename CharType>
typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put(
    iter_type out, std::ios_base& str, char_type fill, unsigned long long value
) const {
  if (str.flags() & std::ios_base::oct) {
    return do_put_int_oct(out, str, fill, (unsigned long long)value);
  }

  if (str.flags() & std::ios_base::hex) {
    static_assert(sizeof(uint64_t) == sizeof(unsigned long long), "sizeof(uint64_t) != sizeof(unsigned long long)");
    return do_put_int_hex(out, str, fill, (uint64_t)value, false);
  }

  if (!value) {
    return do_put_int_zero(out, str,fill); // optimization for '0'
  }

  if ((unsigned long long)irs::integer_traits<int64_t>::const_max < value) {
    throw irs::detailed_io_error("value too large while converting data from UTF8 in num_put_facet::do_put(...)");
  }

  auto ipad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::internal
            ? str.width() : size_t(0)
            ;
  auto rpad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::left
            ? str.width() : size_t(0)
            ;
  auto lpad = !ipad && !rpad ? str.width() : size_t(0);
  size_t size = 0;

  str.width(0); // reset padding

  auto ctx = pool_.emplace(locale_);

  if (!ctx) {
    throw irs::detailed_io_error("failed to retrieve ICU formatter in num_put_facet::do_put(...)");
  }

  static_assert(sizeof(int64_t) == sizeof(long long), "sizeof(int64_t) != sizeof(long long)");
  ctx->reset(str);
  ctx->regular_->format(int64_t(value), ctx->icu_buf0_);

  if (!utf8_.out(ctx->buf_, ctx->icu_buf0_)) {
    throw irs::detailed_io_error("failed to convert data from UTF8 in num_put_facet::do_put(...)");
  }

  size_t len = ctx->buf_.size() + (str.flags() & std::ios_base::showpos ? 1 : 0);

  for (size_t i = lpad < len ? 0 : lpad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  if (str.flags() & std::ios_base::showpos) {
    *out++ = '+';
    ++size;
  }

  for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  for (size_t i = 0, count = ctx->buf_.size(); i < count; ++i) {
    *out++ = ctx->buf_[i];
    ++size;
  }

  for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
    *out++ = fill;
  }

  return out;
}

template<typename CharType>
typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put(
    iter_type out, std::ios_base& str, char_type fill, double value
) const {
  if ((str.flags() & std::ios_base::floatfield) == (std::ios_base::fixed | std::ios_base::scientific)) {
    return do_put_float_hex(out, str, fill, value);
  }

  auto ipad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::internal
            ? str.width() : size_t(0)
            ;
  auto rpad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::left
            ? str.width() : size_t(0)
            ;
  auto lpad = !ipad && !rpad ? str.width() : size_t(0);
  size_t size = 0;

  str.width(0); // reset padding

  auto ctx = pool_.emplace(locale_);

  if (!ctx) {
    throw irs::detailed_io_error("failed to retrieve ICU formatter in num_put_facet::do_put(...)");
  }

  ctx->reset(str);
  ctx->regular_->setMinimumFractionDigits(6); // default 6 as per specification
  ctx->regular_->setMaximumFractionDigits(6); // default 6 as per specification
  ctx->scientific_->setMinimumFractionDigits(6); // default 6 as per specification
  ctx->scientific_->setMaximumFractionDigits(6); // default 6 as per specification

  static const UnicodeString point(".");
  icu::UnicodeString* icu_buf;
  bool negative = false;

  if (value < 0) {
    value = 0 - value;
    negative = true;
  }

  if ((str.flags() & std::ios_base::floatfield) == std::ios_base::fixed) {
    icu::FieldPosition decimal(UNumberFormatFields::UNUM_DECIMAL_SEPARATOR_FIELD);

    // Decimal floating point, lowercase
    ctx->regular_->format(value, ctx->icu_buf0_, decimal);
    icu_buf = &ctx->icu_buf0_;

    if ((str.flags() & std::ios_base::showpoint)
        && !decimal.getBeginIndex() && !decimal.getEndIndex()) { // 0,0 indicates no decimal
      icu_buf->append(point); // append at end
    }
  } else if ((str.flags() & std::ios_base::floatfield) == std::ios_base::scientific) {
    icu::FieldPosition decimal(UNumberFormatFields::UNUM_DECIMAL_SEPARATOR_FIELD);

    // Scientific notation (mantissa/exponent), uppercase/lowercase
    ctx->scientific_->format(value, ctx->icu_buf0_, decimal);
    icu_buf = &ctx->icu_buf0_;

    if ((str.flags() & std::ios_base::showpoint)
        && !decimal.getBeginIndex() && !decimal.getEndIndex()) { // 0,0 indicates no decimal
      icu_buf->insert(icu_buf->length() - 2, point); // -2 to insert before 'e0'
    }
  } else {
    icu::FieldPosition decimal_r(UNumberFormatFields::UNUM_DECIMAL_SEPARATOR_FIELD);
    icu::FieldPosition decimal_s(UNumberFormatFields::UNUM_DECIMAL_SEPARATOR_FIELD);

    // set the maximum number of significant digits to be printed (as per spec)
    ctx->regular_->setMinimumFractionDigits(0);
    ctx->regular_->setMaximumFractionDigits(str.precision());
    ctx->scientific_->setMinimumFractionDigits(0);
    ctx->scientific_->setMaximumFractionDigits(str.precision());

    // Use the shortest representation:
    //  Decimal floating point
    //  Scientific notation (mantissa/exponent), uppercase/lowercase
    ctx->regular_->format(value, ctx->icu_buf0_, decimal_r);
    ctx->scientific_->format(value, ctx->icu_buf1_, decimal_s);

    if ((str.flags() & std::ios_base::showpoint)) {
      if (!decimal_r.getBeginIndex() && !decimal_r.getEndIndex()) { // 0,0 indicates no decimal
        ctx->icu_buf0_.append(point); // append at end
      }

      if (!decimal_s.getBeginIndex() && !decimal_s.getEndIndex()) { // 0,0 indicates no decimal
        ctx->icu_buf1_.insert(ctx->icu_buf1_.length() - 2, point); // -2 to insert before 'e0'
      }
    }

    icu_buf = ctx->icu_buf1_.length() < ctx->icu_buf1_.length()
            ? &ctx->icu_buf1_ : &ctx->icu_buf0_;
  }

  // ensure all letters are uppercased/lowercased
  if (!(str.flags() & std::ios_base::uppercase)) {
    icu_buf->toLower();
  }

  if (!utf8_.out(ctx->buf_, *icu_buf)) {
    throw irs::detailed_io_error("failed to convert data from UTF8 in num_put_facet::do_put(...)");
  }

  size_t len = ctx->buf_.size()
             + (negative || (str.flags() & std::ios_base::showpos) ? 1 : 0);

  for (size_t i = lpad < len ? 0 : lpad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  if (negative) {
    *out++ = '-';
    ++size;
  } else if (str.flags() & std::ios_base::showpos) {
    *out++ = '+';
    ++size;
  }

  for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  for (size_t i = 0, count = ctx->buf_.size(); i < count; ++i) {
    *out++ = ctx->buf_[i];
    ++size;
  }

  for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
    *out++ = fill;
  }

  return out;
}

template<typename CharType>
typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put(
    iter_type out, std::ios_base& str, char_type fill, long double value
) const {
  if ((str.flags() & std::ios_base::floatfield) == (std::ios_base::fixed | std::ios_base::scientific)) {
    return do_put_float_hex(out, str, fill, value);
  }

  // the ICU operations are identical (with lower precision)
  return do_put(out, str, fill, (double)value);
}

template<typename CharType>
typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put(
    iter_type out, std::ios_base& str, char_type fill, const void* value
) const {
  return do_put_int_hex(out, str, fill, size_t(value), true);
}

template<typename CharType>
template<typename T>
/*static*/ typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put_float_hex(
    iter_type out, std::ios_base& str, char_type fill, T value
) {
  typedef typename std::enable_if<std::is_floating_point<T>::value, T>::type type;

  auto ipad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::internal
            ? str.width() : size_t(0)
            ;
  auto rpad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::left
            ? str.width() : size_t(0)
            ;
  auto lpad = !ipad && !rpad ? str.width() : size_t(0);
  size_t size = 0;

  str.width(0); // reset padding

  static auto mantissa_bits = std::numeric_limits<type>::digits;
  static const char lower[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
  static const char upper[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
  auto* table = str.flags() & std::ios_base::uppercase ? upper : lower;
  bool negative = false;

  if (value < 0) {
    value = 0 - value;
    negative = true;
  }

  // optimization for '0'
  if (!value) {
    size_t len = 6 // 0x0p+0
               + ((str.flags() & std::ios_base::showpos) ? 1 : 0)
               + ((str.flags() & std::ios_base::showpoint) ? 1 : 0)
               ;

    for (size_t i = lpad < len ? 0 : lpad - len; i; --i) {
      *out++ = fill;
      ++size;
    }

    // if a sign character occurs in the representation, will pad after the sign
    if (str.flags() & std::ios_base::showpos) {
      *out++ =  '+';
      ++size;

      for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
        *out++ = fill;
        ++len; // subtract from 'ipad'
        ++size;
      }
    }

    *out++ = '0'; // hexadecimal prefix
    *out++ = str.flags() & std::ios_base::uppercase ? 'X' : 'x';
    size += 2;

    for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
      *out++ = fill;
      ++size;
    }

    *out++ = '0';
    ++size;

    if (str.flags() & std::ios_base::showpoint) {
      *out++ = '.';
      ++size;
    }

    *out++ = str.flags() & std::ios_base::uppercase ? 'P' : 'p';
    *out++ = '+';
    *out++ = '0';
    size += 3;

    for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
      *out++ = fill;
    }

    return out;
  }

  int exponent;
  auto mantissa_f = std::frexp(value, &exponent);
  auto mantissa_i = size_t(std::ldexp(mantissa_f, mantissa_bits));
  int half_byte = sizeof(size_t) * 2;

  // strip leading/trailing zero half-bytes
  {
    static_assert(std::numeric_limits<size_t>::digits < irs::integer_traits<int>::const_max, "std::numeric_limits<size_t>::digits >= std::numeric_limits<int>::max()");
    auto clz = int(irs::math::math_traits<size_t>::clz(mantissa_i));
    auto ctz = int(irs::math::math_traits<size_t>::ctz(mantissa_i));

    exponent -=  4 - (clz % 4); // number of bits used in the first half-byte
    half_byte -= clz / 4; // 4 for half-byte
    half_byte -= ctz / 4; // 4 for half-byte
    mantissa_i >>= ctz & ~size_t(0x3); // (ctz / 4) * 4
  }

  auto exp_str = std::to_string(exponent);

  size_t len = half_byte
             + 4 // for 0x...p+
             + (negative || (str.flags() & std::ios_base::showpos) ? 1 : 0)
             + (!half_byte || (str.flags() & std::ios_base::showpoint) ? 1 : 0)
             + exp_str.size()
             ;

  for (size_t i = lpad < len ? 0 : lpad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  // if a sign character occurs in the representation, will pad after the sign
  if (negative || (str.flags() & std::ios_base::showpos)) {
    *out++ = negative ? '-' : '+';
    ++size;

    for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
      *out++ = fill;
      ++len; // subtract from 'ipad'
      ++size;
    }
  }

  *out++ = '0'; // hexadecimal prefix
  *out++ = str.flags() & std::ios_base::uppercase ? 'X' : 'x';
  size += 2;

  for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  bool started = false;

  while(half_byte) {
    auto val = (mantissa_i >> (--half_byte * 4)) & 0xF;

    *out++ = table[val];
    ++size;

    if (!started) {
      started = true;

      if (half_byte || (str.flags() & std::ios_base::showpoint)) {
        *out++ = '.';
        ++size;
      }
    }
  }

  *out++ = str.flags() & std::ios_base::uppercase ? 'P' : 'p'; // exponent suffix
  *out++ = '+';
  size += 2;

  for (size_t i = 0, count = exp_str.size(); i < count; ++i) {
    *out++ = exp_str[i];
    ++size;
  }

  for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
    *out++ = fill;
  }

  return out;
}

template<typename CharType>
template<typename T>
/*static*/ typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put_int_hex(
    iter_type out, std::ios_base& str, char_type fill, T value, bool full_width
) {
  typedef typename std::enable_if<std::is_unsigned<T>::value, T>::type type;

  if (!value && !full_width) {
    return do_put_int_zero(out, str, fill); // optimization for '0'
  }

  auto ipad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::internal
            ? str.width() : size_t(0)
            ;
  auto rpad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::left
            ? str.width() : size_t(0)
            ;
  auto lpad = !ipad && !rpad ? str.width() : size_t(0);
  size_t size = 0;

  str.width(0); // reset padding

  static const char lower[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
  static const char upper[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
  auto* table = str.flags() & std::ios_base::uppercase ? upper : lower;
  auto val = irs::numeric_utils::numeric_traits<type>::hton(value);
  auto* v = reinterpret_cast<uint8_t*>(&val);
  bool started = false;
  size_t len = sizeof(val) * 2 // *2 for hi+lo
             + (str.flags() & std::ios_base::showpos ? 1 : 0)
             + (str.flags() & std::ios_base::showbase ? 2 : 0)
             ;

  for (auto i = sizeof(type); i; --i, ++v) {
    if (started) {
      *out++ = table[*v >> 4];
      *out++ = table[*v & 0xF];
      size += 2;
      continue;
    }

    if (!*v && !full_width) {
      len -= 2; // 2 for hi+lo
      continue;
    }

    auto hi = *v >> 4;
    auto lo = *v & 0xF;

    len -= hi || full_width ? 0 : 1;

    for (size_t i = lpad < len ? 0 : lpad - len; i; --i) {
      *out++ = fill;
      ++size;
    }

    // if a sign character occurs in the representation, will pad after the sign
    if (str.flags() & std::ios_base::showpos) {
      *out++ = '+';
      ++size;

      for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
        *out++ = fill;
        ++len; // subtract from 'ipad'
        ++size;
      }
    }

    // else if representation began with 0x or 0X, will pad after the x or X
    if (str.flags() & std::ios_base::showbase) {
      *out++ = '0'; // hexadecimal prefix
      *out++ = str.flags() & std::ios_base::uppercase ? 'X' : 'x';
      size += 2;
    }

    for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
      *out++ = fill;
      ++size;
    }

    if (hi || full_width) {
      *out++ = table[hi];
      ++size;
    }

    *out++ = table[lo];
    ++size;
    started = true;
  }

  for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
    *out++ = fill;
  }

  return out;
}

template<typename CharType>
template<typename T>
/*static*/ typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put_int_oct(
    iter_type out, std::ios_base& str, char_type fill, T value
) {
  typedef typename std::enable_if<std::is_unsigned<T>::value, T>::type type;

  if (!value) {
    return do_put_int_zero(out, str, fill); // optimization for '0'
  }

  auto ipad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::internal
            ? str.width() : size_t(0)
            ;
  auto rpad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::left
            ? str.width() : size_t(0)
            ;
  auto lpad = !ipad && !rpad ? str.width() : size_t(0);
  size_t size = 0;

  str.width(0); // reset padding

  static const char table[] = { '0', '1', '2', '3', '4', '5', '6', '7' };
  size_t shift = (sizeof(type) * 8 / 3) + 1; // shift in blocks of 3 bits, +1 for initial decrement
  bool started = false;
  size_t len = shift
             + (str.flags() & std::ios_base::showpos ? 1 : 0)
             + (str.flags() & std::ios_base::showbase ? 1 : 0)
             ;

  do {
    auto v = (value >> (--shift * 3)) & 0x7; // shift in blocks of 3 bits

    if (started) {
      *out++ = table[v];
      ++size;
      continue;
    }

    if (!v) {
      --len;
      continue;
    }

    for (size_t i = lpad < len ? 0 : lpad - len; i; --i) {
      *out++ = fill;
      ++size;
    }

    if (str.flags() & std::ios_base::showpos) {
      *out++ = '+';
      ++size;
    }

    for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
      *out++ = fill;
      ++len; // subtract from 'ipad'
      ++size;
    }

    if (str.flags() & std::ios_base::showbase) {
      *out++ = '0'; // octal prefix
      ++size;
    }

    *out++ = table[v];
    ++size;
    started = true;
  } while (shift);

  for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
    *out++ = fill;
  }

  return out;
}

template<typename CharType>
/*static*/ typename num_put_facet<CharType>::iter_type num_put_facet<CharType>::do_put_int_zero(
    iter_type out, std::ios_base& str, char_type fill
) {
  auto ipad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::internal
            ? str.width() : size_t(0)
            ;
  auto rpad = (str.flags() & std::ios_base::adjustfield) == std::ios_base::left
            ? str.width() : size_t(0)
            ;
  auto lpad = !ipad && !rpad ? str.width() : size_t(0);
  size_t size = 0;

  str.width(0); // reset padding

  size_t len = strlen("0") + (str.flags() & std::ios_base::showpos ? 1 : 0);

  for (size_t i = lpad < len ? 0 : lpad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  if (str.flags() & std::ios_base::showpos) {
    *out++ = '+';
    ++size;
  }

  for (size_t i = ipad < len ? 0 : ipad - len; i; --i) {
    *out++ = fill;
    ++size;
  }

  *out++ = '0';
  ++size;

  for (size_t i = rpad < size ? 0 : rpad - size; i; --i) {
    *out++ = fill;
  }

  return out;
}

class num_putw_facet: public std::num_put<wchar_t> {
  // FIXME TODO implement
};

class numpunct_facet: public std::numpunct<char> {
  // FIXME TODO implement
};

class numpunctw_facet: public std::numpunct<wchar_t> {
  // FIXME TODO implement
};

class time_get_facet: public std::time_get<char> {
  // FIXME TODO implement
};

class time_getw_facet: public std::time_get<wchar_t> {
  // FIXME TODO implement
};

class time_put_facet: public std::time_put<char> {
  // FIXME TODO implement
};

class time_putw_facet: public std::time_put<wchar_t> {
  // FIXME TODO implement
};

class messages_facet: public std::messages<char> {
  // FIXME TODO implement
};

class messagesw_facet: public std::messages<wchar_t> {
  // FIXME TODO implement
};

// -----------------------------------------------------------------------------
// --SECTION--                                         custom std::locale facets
// -----------------------------------------------------------------------------

class locale_info_facet: public std::locale::facet {
 public:
  static std::locale::id id; // required for each class derived from std::locale::facet as per spec

  locale_info_facet(const irs::string_ref& name);
  locale_info_facet(locale_info_facet const& other) = delete; // because of string_ref
  locale_info_facet(locale_info_facet&& other) NOEXCEPT { *this = std::move(other); }
  locale_info_facet& operator=(const locale_info_facet& other) = delete; // because of string_ref
  locale_info_facet& operator=(locale_info_facet&& other) NOEXCEPT;
  bool operator<(const locale_info_facet& other) const NOEXCEPT { return name_ < other.name_; }
  const irs::string_ref& country() const NOEXCEPT { return country_; }
  const irs::string_ref& encoding() const NOEXCEPT { return encoding_; }
  const irs::string_ref& language() const NOEXCEPT { return language_; }
  const std::string& name() const NOEXCEPT { return name_; }
  bool utf8() const NOEXCEPT { return utf8_; }
  const irs::string_ref& variant() const NOEXCEPT { return variant_; }

 private:
  std::string name_; // the normalized locale name: language[_COUNTRY][.encoding][@variant]
  irs::string_ref country_;
  irs::string_ref encoding_;
  irs::string_ref language_;
  irs::string_ref variant_;
  bool utf8_;
};

/*static*/ std::locale::id locale_info_facet::id;

//////////////////////////////////////////////////////////////////////////////
/// The name has the following format: language[_COUNTRY][.encoding][@variant]
/// Where 'language' is ISO-639 language code like "en" or "ru",
/// 'COUNTRY' is ISO-3166 country identifier like "US" or "RU",
/// 'encoding' is a charracter set name like "UTF-8" or "ISO-8859-1",
/// 'variant' is backend specific variant like "euro" or "calendar=hebrew"
//////////////////////////////////////////////////////////////////////////////
locale_info_facet::locale_info_facet(const irs::string_ref& name)
  : name_(name),
    country_(""),
    encoding_("us-ascii"),
    language_("C"),
    variant_(""),
    utf8_(false) { // us-ascii is not utf8
  if (name_.empty() || name_ == "C") {
    return;
  }

  if (name_ == "c") {
    name_ = "C"; // uppercase 'classic' locale name

    return;
  }

  auto data = &name_[0];
  std::transform(data, data + name_.size(), data, ::tolower); // lowercase full string
  auto length = ::strcspn(data, "-_.@");

  language_ = irs::string_ref(data, length);
  data += length;

  // found country
  if ('-' == data[0] || '_' == data[0]) {
    ++data;
    length = ::strcspn(data, ".@");
    country_ = irs::string_ref(data, length);
    std::transform(data, data + length, data, ::toupper); // uppercase country
    data += length;
  }

  // found encoding
  if ('.' == data[0]) {
    ++data;
    length = ::strcspn(data, "@");
    encoding_ = irs::string_ref(data, length);
    data += length;

    // normalize encoding and compare to 'utf8' (data already in lower case)
    std::string buf = encoding_;
    auto* str = &buf[0];
    auto end = std::remove_if(
      str, str + buf.size(),
      [](char x){ return !(('0' <= x && '9' >= x) || ('a' <= x && 'z' >= x)); }
    );
    irs::string_ref enc(str, std::distance(str, end));

    utf8_ = enc == "utf8";
  }

  // found variant
  if ('@' == data[0]) {
    ++data;
    variant_ = data;
  }
}

locale_info_facet& locale_info_facet::operator=(
    locale_info_facet&& other
) NOEXCEPT {
  if (this != &other) {
    const char* start = &(other.name_[0]);
    const char* end = start + other.name_.size();

    name_ = std::move(other.name_); // move first since string_ref point into it

    country_ = other.country_.c_str() < start || other.country_.c_str() >= end
             ? other.country_ // does not point into 'name_'
             : irs::string_ref(
                 &name_[0] + std::distance(start, other.country_.c_str()),
                 other.country_.size()
               )
             ;

    encoding_ = other.encoding_.c_str() < start || other.encoding_.c_str() >= end
              ? other.encoding_ // does not point into 'name_'
              : irs::string_ref(
                  &name_[0] + std::distance(start, other.encoding_.c_str()),
                  other.encoding_.size()
                )
              ;

    language_ = other.language_.c_str() < start || other.language_.c_str() >= end
              ? other.language_ // does not point into 'name_'
              : irs::string_ref(
                  &name_[0] + std::distance(start, other.language_.c_str()),
                  other.language_.size()
                )
              ;

    variant_ = other.variant_.c_str() < start || other.variant_.c_str() >= end
             ? other.variant_ // does not point into 'name_'
             : irs::string_ref(
                 &name_[0] + std::distance(start, other.variant_.c_str()),
                 other.variant_.size()
               )
             ;

    utf8_ = other.utf8_;
    other.country_ = irs::string_ref::NIL;
    other.encoding_ = irs::string_ref::NIL;
    other.language_ = irs::string_ref::NIL;
    other.variant_ = irs::string_ref::NIL;
  }

  return *this;
}

// FIXME TODO remove 'base' once converters are implemented
const std::locale& get_encoding(std::string&& encoding, const std::locale& base) {
  static std::map<std::string, std::locale> encodings;
  static std::mutex mutex;
  SCOPED_LOCK(mutex);
  auto itr = encodings.find(encoding);

  if (itr != encodings.end()) {
    return itr->second;
  }

  // FIXME TODO replace first implementation with second once converters are implemented
  auto codecvt_char = irs::memory::make_unique<codecvt_facet<char>>(encoding);
  auto locale_char = std::locale(std::locale::classic(), codecvt_char.release());
  //auto locale_char16 = locale_char.combine<std::codecvt<char16_t, char, mbstate_t>>(base);
  //auto locale_char32 = locale_char16.combine<std::codecvt<char32_t, char, mbstate_t>>(base);
  auto& locale_char32 = locale_char;
  auto locale_wchar = locale_char32.combine<std::codecvt<wchar_t, char, mbstate_t>>(base);
  /*use below
  auto codecvt_char = irs::memory::make_unique<codecvt_facet>(encoding);
  auto codecvt_char16 = irs::memory::make_unique<codecvt16_facet>(encoding);
  auto codecvt_char32 = irs::memory::make_unique<codecvt32_facet>(encoding);
  auto codecvt_wchar = irs::memory::make_unique<codecvtw_facet>(encoding);
  auto locale_char = std::locale(std::locale::classic(), codecvt_char.release());
  auto locale_char16 = std::locale(locale_char, codecvt_char16.release());
  auto locale_char32 = std::locale(locale_char16, codecvt_char32.release());
  auto locale_wchar = std::locale(locale_char32, codecvt_wchar.release());
  */

  return encodings.emplace(std::move(encoding), locale_wchar).first->second;
}

const std::locale& get_locale(
    const irs::string_ref& name, bool forceUnicodeSystem = true
) {
  if (name.null() && !forceUnicodeSystem) {
    return std::locale::classic();
  }

  struct less_t {
    bool operator()(
        const locale_info_facet* lhs, const locale_info_facet* rhs
    ) const NOEXCEPT {
      return (!lhs && rhs) || (lhs && rhs && *lhs < *rhs);
    }
  };

  locale_info_facet info(name);
  static std::map<locale_info_facet*, std::locale, less_t> locales;
  static std::mutex mutex;
  SCOPED_LOCK(mutex);
  auto itr = locales.find(&info);

  if (itr != locales.end()) {
    return itr->second;
  }

  // Boost locales always assume system is unicode
  boost::locale::generator locale_genrator; // stateful object, cannot be static
  icu::Locale icu_locale(
    std::string(info.language()).c_str(),
    std::string(info.country()).c_str(),
    std::string(info.variant()).c_str()
  );

  if (icu_locale.isBogus()) {
    IR_FRMT_WARN("locale '%s' is not supported by ICU", info.name().c_str());
  }

  auto boost_locale = locale_genrator.generate(info.name());
  auto encoding = get_encoding(info.encoding(), boost_locale);
  auto& codecvt_char = static_cast<const codecvt_facet<char>&>( // get_encoding(...) adds codecvt_facet<char>
    std::use_facet<std::codecvt<char, char, mbstate_t>>(encoding)
  );
  auto locale_info =
    irs::memory::make_unique<locale_info_facet>(std::move(info));
  auto* locale_info_ptr = locale_info.get();
  auto& converter = get_converter(locale_info->encoding());
  auto locale = std::locale(boost_locale, locale_info.release());

  locale = std::locale(
    locale, irs::memory::make_unique<codecvt16_facet>(converter).release()
  );
  locale = std::locale(
    locale, irs::memory::make_unique<codecvt32_facet>(converter).release()
  );

  if (forceUnicodeSystem) {
/* FIXME TODO enable
    locale = std::locale(
      locale, irs::memory::make_unique<codecvt8u_facet>(converter).release()
    );
*/
    locale = std::locale(
      locale, irs::memory::make_unique<codecvtwu_facet>(converter).release()
    );
  } else {
/* FIXME TODO enable
    locale = std::locale(
      locale, irs::memory::make_unique<codecvt8_facet>(converter).release()
    );
    locale = std::locale(
      locale, irs::memory::make_unique<codecvtw_facet>(converter).release()
    );
*/
  }

  locale = std::locale(
    locale,
    irs::memory::make_unique<num_put_facet<char>>(
      icu_locale, codecvt_char
    ).release()
  );

  return locales.emplace(locale_info_ptr, locale).first->second;
}

NS_END

NS_ROOT
NS_BEGIN( locale_utils )

const irs::string_ref& country(std::locale const& locale) {
  auto* loc = &locale;

  if (!std::has_facet<locale_info_facet>(*loc)) {
    loc = &get_locale(loc->name());
  }

  return std::use_facet<locale_info_facet>(*loc).country();
}

const irs::string_ref& encoding(std::locale const& locale) {
  auto* loc = &locale;

  if (!std::has_facet<locale_info_facet>(*loc)) {
    loc = &get_locale(loc->name());
  }

  return std::use_facet<locale_info_facet>(*loc).encoding();
}

const irs::string_ref& language(std::locale const& locale) {
  auto* loc = &locale;

  if (!std::has_facet<locale_info_facet>(*loc)) {
    loc = &get_locale(loc->name());
  }

  return std::use_facet<locale_info_facet>(*loc).language();
}

std::locale locale(char const* czName, bool bForceUTF8 /*= false*/) {
  if (!czName) {
    return bForceUTF8
      ? locale(std::locale::classic().name(), true)
      : std::locale::classic()
      ;
  }

  const std::string sName(czName);

  return locale(sName, bForceUTF8);
}

std::locale locale(std::string const& sName, bool bForceUTF8 /*= false*/) {
  if (!bForceUTF8) {
    return get_locale(sName);
  }

  // ensure boost::locale::info facet exists for 'sName' since it is used below
  auto locale = get_locale(sName);
  auto locale_info = boost::locale::util::create_info(locale, sName);
  auto& info_facet = std::use_facet<boost::locale::info>(locale_info);

  if (info_facet.utf8()) {
    return locale;
  }

  return iresearch::locale_utils::locale(
    info_facet.language(),
    info_facet.country(),
    "UTF-8",
    info_facet.variant()
  );
}

std::locale locale(
    irs::string_ref const& name,
    irs::string_ref const& encodingOverride /*= irs::string_ref::NIL*/,
    bool forceUnicodeSystem /*= true*/
) {
  if (encodingOverride.null()) {
    return get_locale(name, forceUnicodeSystem);
  }

  locale_info_facet info(name);
  std::string locale_name = info.language();

  if (!info.country().empty()) {
    locale_name.append(1, '_').append(info.country());
  }

  if (!encodingOverride.empty()) {
    locale_name.append(1, '.').append(encodingOverride);
  }

  if (!info.variant().empty()) {
    locale_name.append(1, '@').append(info.variant());
  }

  return get_locale(locale_name, forceUnicodeSystem);
}

std::locale locale(std::string const& sLanguage, std::string const& sCountry, std::string const& sEncoding, std::string const& sVariant /*= ""*/) {
  bool bValid = sLanguage.find('_') == std::string::npos && sCountry.find('_') == std::string::npos && sEncoding.find('_') == std::string::npos && sVariant.find('_') == std::string::npos
              && sLanguage.find('.') == std::string::npos && sCountry.find('.') == std::string::npos && sEncoding.find('.') == std::string::npos && sVariant.find('.') == std::string::npos
              && sLanguage.find('@') == std::string::npos && sCountry.find('@') == std::string::npos && sEncoding.find('@') == std::string::npos && sVariant.find('@') == std::string::npos;
  std::string sName = sLanguage;// sLanguage.empty() && sCountry.empty() && (!sEncoding.empty() || !sVariant.empty()) ? "C" : sLanguage;

  if (!bValid || !sCountry.empty()) {
    sName.append(1, '_').append(sCountry);
  }

  if (!bValid || !sEncoding.empty()) {
    sName.append(1, '.').append(sEncoding);
  }

  if (!bValid || !sVariant.empty()) {
    sName.append(1, '@').append(sVariant);
  }

  return iresearch::locale_utils::locale(sName);
}

const std::string& name(std::locale const& locale) {
  auto* loc = &locale;

  if (!std::has_facet<locale_info_facet>(*loc)) {
    loc = &get_locale(loc->name());
  }

  return std::use_facet<locale_info_facet>(*loc).name();
}

bool utf8(std::locale const& locale) {
  auto* loc = &locale;

  if (!std::has_facet<locale_info_facet>(*loc)) {
    loc = &get_locale(loc->name());
  }

  return std::use_facet<locale_info_facet>(*loc).utf8();
}

NS_END // locale_utils
NS_END

NS_BEGIN(std)

// MSVC2015/MSVC2017 implementations do not support char16_t/char32_t 'codecvt'
// due to a missing export, as per their comment:
//   This is an active bug in our database (VSO#143857), which we'll investigate
//   for a future release, but we're currently working on higher priority things
// define the missing variables for at least the static-CRT implementations
#if defined(_MSC_VER) && _MSC_VER > 1800 && !defined(_DLL)
  std::locale::id std::codecvt<char16_t, char, _Mbstatet>::id;
  std::locale::id std::codecvt<char32_t, char, _Mbstatet>::id;
#endif

NS_END // std

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------