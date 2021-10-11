// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#ifndef BOOST_TEXT_DATA_FA_HPP
#define BOOST_TEXT_DATA_FA_HPP

#include <boost/text/string_view.hpp>


namespace boost { namespace text { namespace data { namespace fa {

inline string_view standard_collation_tailoring()
{
    return string_view((char const *)
u8R"(  
[normalization on]
[reorder Arab]
&َ<<ِ<<ُ<<ً<<ٍ<<ٌ
&[before 1]ا<آ
&ا<<ٱ<ء
  <<أ<<ٲ<<إ<<ٳ<<ؤ
  <<یٔ<<<ىٔ<<<ئ
&ک<<*ڪګكڬڭڮ
&ۏ<ه<<ە<<ہ<<ة<<ۃ<<ۀ<<ھ
&ی<<*ىےيېۑۍێ
  )");
}


}}}}

#endif
