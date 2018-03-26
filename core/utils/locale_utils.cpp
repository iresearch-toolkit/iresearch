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

#include <algorithm>
#include <cstring>
#include <map>

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

#include "object_pool.hpp"
#include "numeric_utils.hpp"
#include "error/error.hpp"

#include "locale_utils.hpp"

NS_LOCAL

////////////////////////////////////////////////////////////////////////////////
/// @brief size of ICU object pools, arbitrary size
////////////////////////////////////////////////////////////////////////////////
const size_t POOL_SIZE = 8;

// -----------------------------------------------------------------------------
// --SECTION--                                    facets required by std::locale
// -----------------------------------------------------------------------------

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

class codecvt16_facet: public std::codecvt<char16_t, char, mbstate_t> {
  // FIXME TODO implement
};

class codecvt32_facet: public std::codecvt<char32_t, char, mbstate_t> {
  // FIXME TODO implement
};

class codecvtw_facet: public std::codecvt<wchar_t, char, mbstate_t> {
  // FIXME TODO implement
};

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

const std::locale& get_locale(const irs::string_ref& name) {
  if (name.null()) {
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
  auto num_put_char = irs::memory::make_unique<num_put_facet<char>>(
    icu_locale, codecvt_char
  );
  auto locale = std::locale(boost_locale, locale_info.release());

  locale = std::locale(locale, num_put_char.release());

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

NS_END
NS_END

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------