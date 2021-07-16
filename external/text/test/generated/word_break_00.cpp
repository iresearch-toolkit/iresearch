// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#include <boost/text/word_break.hpp>

#include <gtest/gtest.h>

#include <algorithm>


TEST(word, breaks_0)
{
    // ÷ 0001 ÷ 0001 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] <START OF HEADING> (Other) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x1 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 0001 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] <START OF HEADING> (Other) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x1 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 000D ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [3.2] <CARRIAGE RETURN (CR)> (CR) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0xd }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 000D ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [3.2] <CARRIAGE RETURN (CR)> (CR) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0xd }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 000A ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [3.2] <LINE FEED (LF)> (LF) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0xa }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 000A ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [3.2] <LINE FEED (LF)> (LF) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0xa }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 000B ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [3.2] <LINE TABULATION> (Newline) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0xb }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 000B ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [3.2] <LINE TABULATION> (Newline) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0xb }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 3031 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] VERTICAL KANA REPEAT MARK (Katakana) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x3031 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 3031 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] VERTICAL KANA REPEAT MARK (Katakana) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x3031 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 0041 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] LATIN CAPITAL LETTER A (ALetter) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x41 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 0041 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] LATIN CAPITAL LETTER A (ALetter) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x41 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 003A ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] COLON (MidLetter) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x3a }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 003A ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] COLON (MidLetter) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x3a }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 002C ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] COMMA (MidNum) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x2c }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 002C ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] COMMA (MidNum) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x2c }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 002E ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] FULL STOP (MidNumLet) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x2e }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 002E ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] FULL STOP (MidNumLet) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x2e }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 0030 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] DIGIT ZERO (Numeric) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x30 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 0030 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] DIGIT ZERO (Numeric) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x30 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 005F ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] LOW LINE (ExtendNumLet) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x5f }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 005F ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] LOW LINE (ExtendNumLet) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x5f }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 1F1E6 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] REGIONAL INDICATOR SYMBOL LETTER A (RI) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x1f1e6 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 1F1E6 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] REGIONAL INDICATOR SYMBOL LETTER A (RI) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x1f1e6 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


    // ÷ 0001 ÷ 05D0 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) ÷ [999.0] HEBREW LETTER ALEF (Hebrew_Letter) ÷ [0.3]
    {
        std::array<uint32_t, 2> cps = {{ 0x1, 0x5d0 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 1);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 1, cps.end()) - cps.begin(), 2);
    }


    // ÷ 0001 × 0308 ÷ 05D0 ÷	
    // ÷ [0.2] <START OF HEADING> (Other) × [4.0] COMBINING DIAERESIS (Extend_FE) ÷ [999.0] HEBREW LETTER ALEF (Hebrew_Letter) ÷ [0.3]
    {
        std::array<uint32_t, 3> cps = {{ 0x1, 0x308, 0x5d0 }};
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 0, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 1, cps.end()) - cps.begin(), 0);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 0, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 2, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
        EXPECT_EQ(boost::text::prev_word_break(cps.begin(), cps.begin() + 3, cps.end()) - cps.begin(), 2);
        EXPECT_EQ(boost::text::next_word_break(cps.begin() + 2, cps.end()) - cps.begin(), 3);
    }


}
