// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
#include <boost/text/collation_table.hpp>
#include <boost/text/collate.hpp>
#include <boost/text/data/all.hpp>

#include <gtest/gtest.h>

#include <fstream>


using namespace boost;

namespace std {
    ostream & operator<<(ostream & os, std::array<uint32_t, 1> cp)
    {
        return os << hex << "0x" << cp[0] << dec;
    }
    ostream &
    operator<<(ostream & os, container::static_vector<uint32_t, 16> const & vec)
    {
        os << "{ " << hex;
        for (uint32_t cp : vec) {
            os << "0x" << cp << " ";
        }
        os << "}" << dec;
        return os;
    }
    template<typename Iter, typename Sentinel>
    ostream &
    operator<<(ostream & os, text::utf32_view<Iter, Sentinel> const & r)
    {
        os << '"' << hex;
        for (uint32_t cp : r) {
            if (cp < 0x80)
                os << (char)cp;
            else if (cp <= 0xffff)
                os << "\\u" << setw(4) << setfill('0') << cp;
            else
                os << "\\U" << setw(8) << setfill('0') << cp;
        }
        os << '"' << dec;
        return os;
    }
}

#if !BOOST_TEXT_COLLATE_INSTRUMENTATION
namespace boost { namespace text {
    inline std::ostream & operator<<(std::ostream & os, text_sort_key const & k)
    {
        os << std::hex << "[";
        for (auto x : k) {
            os << " " << x;
        }
        os << " ]" << std::dec;
        return os;
    }
}}
#endif


TEST(tailoring, case_first)
{
    std::array<std::array<uint32_t, 3>, 4> const default_ordering = {{
        {{0x61, 0x62, 0x63}}, // "abc"
        {{0x61, 0x62, 0x43}}, // "abC"
        {{0x61, 0x42, 0x63}}, // "aBc"
        {{0x41, 0x62, 0x63}}, // "Abc"
    }};

    text::collation_table const default_table = text::default_collation_table();

    {
        auto this_ordering = default_ordering;
        std::sort(
            this_ordering.begin(),
            this_ordering.end(),
            default_table.compare());
        EXPECT_EQ(this_ordering, default_ordering);
    }

    {
        text::collation_table const case_first_off =
            text::tailored_collation_table("[caseFirst off]");

        auto this_ordering = default_ordering;
        std::sort(
            this_ordering.begin(),
            this_ordering.end(),
            case_first_off.compare());
        EXPECT_EQ(this_ordering, default_ordering);
    }

    {
        text::collation_table const lower_first =
            text::tailored_collation_table("[caseFirst lower]");

        auto this_ordering = default_ordering;
        std::sort(
            this_ordering.begin(), this_ordering.end(), lower_first.compare());
        EXPECT_EQ(this_ordering, default_ordering);
    }

    {
        text::collation_table const upper_first =
            text::tailored_collation_table("[caseFirst upper]");

        auto this_ordering = default_ordering;
        std::sort(
            this_ordering.begin(), this_ordering.end(), upper_first.compare());
        EXPECT_NE(this_ordering, default_ordering);
        std::reverse(this_ordering.begin(), this_ordering.end());
        EXPECT_EQ(this_ordering, default_ordering);
    }
}

TEST(tailoring, case_level)
{
    // Default collation indicates: role <2 rôle <3 Rôle
    std::array<std::array<uint32_t, 4>, 4> const strings = {{
        {{0x72, 0x6f, 0x6c, 0x65}}, // "role"
        {{0x72, 0xf4, 0x6c, 0x65}}, // "rôle"
        {{0x52, 0xf4, 0x6c, 0x65}}, // "Rôle"
        {{0x52, 0x6f, 0x6c, 0x65}}, // "Role"
    }};

    text::collation_table const default_table = text::default_collation_table();

    {
        auto const primary_less =
            default_table.compare(text::collation_strength::primary);
        EXPECT_FALSE(primary_less(strings[0], strings[1]));
        EXPECT_FALSE(primary_less(strings[1], strings[0]));
        EXPECT_FALSE(primary_less(strings[0], strings[2]));
        EXPECT_FALSE(primary_less(strings[2], strings[0]));
        EXPECT_FALSE(primary_less(strings[0], strings[3]));
        EXPECT_FALSE(primary_less(strings[3], strings[0]));
        auto const secondary_less =
            default_table.compare(text::collation_strength::secondary);
        EXPECT_TRUE(secondary_less(strings[0], strings[1]));
        EXPECT_FALSE(secondary_less(strings[1], strings[2]));
        auto const tertiary_less =
            default_table.compare(text::collation_strength::tertiary);
        EXPECT_TRUE(tertiary_less(strings[0], strings[2]));
        EXPECT_TRUE(tertiary_less(strings[1], strings[2]));
    }

    {
        text::collation_table const case_level_off =
            text::tailored_collation_table("[caseLevel off]");

        auto const primary_less =
            case_level_off.compare(text::collation_strength::primary);
        EXPECT_FALSE(primary_less(strings[0], strings[1]));
        EXPECT_FALSE(primary_less(strings[1], strings[0]));
        EXPECT_FALSE(primary_less(strings[0], strings[2]));
        EXPECT_FALSE(primary_less(strings[2], strings[0]));
        EXPECT_FALSE(primary_less(strings[0], strings[3]));
        EXPECT_FALSE(primary_less(strings[3], strings[0]));
        auto const secondary_less =
            case_level_off.compare(text::collation_strength::secondary);
        EXPECT_TRUE(secondary_less(strings[0], strings[1]));
        EXPECT_FALSE(secondary_less(strings[1], strings[2]));
        auto const tertiary_less =
            case_level_off.compare(text::collation_strength::tertiary);
        EXPECT_TRUE(tertiary_less(strings[0], strings[2]));
        EXPECT_TRUE(tertiary_less(strings[1], strings[2]));
    }

    {
        text::collation_table const case_level_on =
            text::tailored_collation_table("[caseLevel on]");

        auto const primary_less =
            case_level_on.compare(text::collation_strength::primary);
        EXPECT_FALSE(primary_less(strings[0], strings[1]));
        EXPECT_FALSE(primary_less(strings[1], strings[0]));
        EXPECT_TRUE(primary_less(strings[0], strings[2]));
        EXPECT_FALSE(primary_less(strings[2], strings[0]));
        EXPECT_TRUE(primary_less(strings[0], strings[3]));
        EXPECT_FALSE(primary_less(strings[3], strings[0]));
        auto const secondary_less =
            case_level_on.compare(text::collation_strength::secondary);
        EXPECT_TRUE(secondary_less(strings[0], strings[1]));
        EXPECT_TRUE(secondary_less(strings[1], strings[2]));
        auto const tertiary_less =
            case_level_on.compare(text::collation_strength::tertiary);
        EXPECT_TRUE(tertiary_less(strings[0], strings[2]));
        EXPECT_TRUE(tertiary_less(strings[1], strings[2]));
    }
}

// First two and last two of each reorder group, and a sampling of implicits.
constexpr std::array<uint32_t, 1> space[4] = {
    {{0x0009}}, {{0x000A}}, {{0x2007}}, {{0x202F}}};
constexpr std::array<uint32_t, 1> digit[4] = {
    {{0x09F4}}, {{0x09F5}}, {{0x32C8}}, {{0x3361}}};
constexpr std::array<uint32_t, 1> Latn[4] = {
    {{0x0061}}, {{0xFF41}}, {{0x02AC}}, {{0x02AD}}};
constexpr std::array<uint32_t, 1> Grek[4] = {
    {{0x03B1}}, {{0x1D6C2}}, {{0x03F8}}, {{0x03F7}}};
constexpr std::array<uint32_t, 1> Copt[4] = {
    {{0x2C81}}, {{0x2C80}}, {{0x2CE3}}, {{0x2CE2}}};
constexpr std::array<uint32_t, 1> Hani[4] = {
    {{0x2F00}}, {{0x3280}}, {{0x2F88F}}, {{0x2FA1D}}};

constexpr std::array<uint32_t, 1> implicit[4] = {
    {{0x2a700}}, {{0x2b740}}, {{0x2b820}}, {{0x2ebe0}}};

struct reordering_t
{
    text::string_view name_;
    std::array<uint32_t, 1> const * cps_;

    friend bool operator<(reordering_t lhs, reordering_t rhs)
    {
        return lhs.name_ < rhs.name_;
    }
};


TEST(tailoring, reordering)
{
    std::array<reordering_t, 5> reorderings{{
        {"space", space},
        {"digit", digit},
        {"Latn", Latn},
        {"Grek", Grek},
        //{"Copt", Copt}, // This works, but makes the test take way too long.
        {"Hani", Hani},
    }};

    std::sort(reorderings.begin(), reorderings.end());

    std::string reordering_str;
    std::vector<std::array<uint32_t, 1>> cps;
    do {
        reordering_str = "[reorder";
        cps.clear();
        for (auto reorder : reorderings) {
            reordering_str += " ";
            reordering_str.append(reorder.name_.begin(), reorder.name_.end());
            cps.insert(cps.end(), reorder.cps_, reorder.cps_ + 4);
            if (reorder.name_ == "Hani")
                cps.insert(cps.end(), implicit, implicit + 4);
        }
        reordering_str += "]";

        text::collation_table const table = text::tailored_collation_table(
            reordering_str,
            "reorderings",
            [](std::string const & s) { std::cout << s; },
            [](std::string const & s) { std::cout << s; });

        for (int i = 0, end = (int)cps.size() - 1; i != end; ++i) {
            EXPECT_LE(
                text::collate(
                    cps[i].begin(),
                    cps[i].end(),
                    cps[i + 1].begin(),
                    cps[i + 1].end(),
                    table,
                    text::collation_strength::primary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                0)
                << reordering_str << " " << cps[i] << " " << cps[i + 1];
        }
    } while (std::next_permutation(reorderings.begin(), reorderings.end()));
}

TEST(tailoring, de)
{
    // Looks like the default de collation is the default collation.
    text::collation_table const table = text::default_collation_table();

    int const cases = 12;

    std::array<container::static_vector<uint32_t, 16>, cases> const lhs = {
        {{0x47, 0x72, 0x00f6, 0x00df, 0x65},
         {0x61, 0x62, 0x63},
         {0x54, 0x00f6, 0x6e, 0x65},
         {0x54, 0x00f6, 0x6e, 0x65},
         {0x54, 0x00f6, 0x6e, 0x65},
         {0x61, 0x0308, 0x62, 0x63},
         {0x00e4, 0x62, 0x63},
         {0x00e4, 0x62, 0x63},
         {0x53, 0x74, 0x72, 0x61, 0x00df, 0x65},
         {0x65, 0x66, 0x67},
         {0x00e4, 0x62, 0x63},
         {0x53, 0x74, 0x72, 0x61, 0x00df, 0x65}}};

    std::array<container::static_vector<uint32_t, 16>, cases> const rhs = {
        {{0x47, 0x72, 0x6f, 0x73, 0x73, 0x69, 0x73, 0x74},
         {0x61, 0x0308, 0x62, 0x63},
         {0x54, 0x6f, 0x6e},
         {0x54, 0x6f, 0x64},
         {0x54, 0x6f, 0x66, 0x75},
         {0x41, 0x0308, 0x62, 0x63},
         {0x61, 0x0308, 0x62, 0x63},
         {0x61, 0x65, 0x62, 0x63},
         {0x53, 0x74, 0x72, 0x61, 0x73, 0x73, 0x65},
         {0x65, 0x66, 0x67},
         {0x61, 0x65, 0x62, 0x63},
         {0x53, 0x74, 0x72, 0x61, 0x73, 0x73, 0x65}}};

    std::array<int, cases> const primary_result = {
        {-1, 0, 1, 1, 1, 0, 0, -1, 0, 0, -1, 0}};

    std::array<int, cases> const tertiary_result = {
        {-1, -1, 1, 1, 1, -1, 0, -1, 1, 0, -1, 1}};

    for (int i = 0; i < cases; ++i) {
        EXPECT_EQ(
            text::collate(
                lhs[i].begin(),
                lhs[i].end(),
                rhs[i].begin(),
                rhs[i].end(),
                table,
                text::collation_strength::primary,
                text::case_first::off,
                text::case_level::off,
                text::variable_weighting::non_ignorable),
            primary_result[i])
            << "CASE " << i << "\n"
            << lhs[i] << "\n"
            << rhs[i] << "\n";
        EXPECT_EQ(
            text::collate(
                lhs[i].begin(),
                lhs[i].end(),
                rhs[i].begin(),
                rhs[i].end(),
                table,
                text::collation_strength::tertiary,
                text::case_first::off,
                text::case_level::off,
                text::variable_weighting::non_ignorable),
            tertiary_result[i])
            << "CASE " << i << "\n"
            << lhs[i] << "\n"
            << rhs[i] << "\n";
    }
}

TEST(tailoring, en)
{
    // The standard English collation is just the default collation.
    text::collation_table const table = text::default_collation_table();

    {
        int const cases = 49;

        std::array<container::static_vector<uint32_t, 16>, cases> const lhs = {
            {{0x0061, 0x0062},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x002d,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x0020,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x002d,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0048, 0x0065, 0x006c, 0x006c, 0x006f},
             {0x0041, 0x0042, 0x0043},
             {0x0061, 0x0062, 0x0063},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x002d,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x002d,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0070, 0x00ea, 0x0063, 0x0068, 0x0065},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x00e9},
             {0x00c4, 0x0042, 0x0308, 0x0043, 0x0308},
             {0x0061, 0x0308, 0x0062, 0x0063},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x0065, 0x0072},
             {0x0072, 0x006f, 0x006c, 0x0065, 0x0073},
             {0x0061, 0x0062, 0x0063},
             {0x0041},
             {0x0041},
             {0x0061, 0x0062},
             {0x0074,
              0x0063,
              0x006f,
              0x006d,
              0x0070,
              0x0061,
              0x0072,
              0x0065,
              0x0070,
              0x006c,
              0x0061,
              0x0069,
              0x006e},
             {0x0061, 0x0062},
             {0x0061, 0x0023, 0x0062},
             {0x0061, 0x0023, 0x0062},
             {0x0061, 0x0062, 0x0063},
             {0x0041, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x0061, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x0061, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x00e6, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x00e4, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0063, 0x0048, 0x0063},
             {0x0061, 0x0308, 0x0062, 0x0063},
             {0x0074, 0x0068, 0x0069, 0x0302, 0x0073},
             {0x0070, 0x00ea, 0x0063, 0x0068, 0x0065},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x00e6, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x00e6, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x00e9}}};

        std::array<container::static_vector<uint32_t, 16>, cases> const rhs = {
            {{0x0061, 0x0062, 0x0063},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x002d,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0062, 0x006c, 0x0061, 0x0063, 0x006b},
             {0x0068, 0x0065, 0x006c, 0x006c, 0x006f},
             {0x0041, 0x0042, 0x0043},
             {0x0041, 0x0042, 0x0043},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x0062,
              0x0069,
              0x0072,
              0x0064,
              0x0073},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x0062,
              0x0069,
              0x0072,
              0x0064,
              0x0073},
             {0x0062,
              0x006c,
              0x0061,
              0x0063,
              0x006b,
              0x0062,
              0x0069,
              0x0072,
              0x0064},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x00e9},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x0065, 0x0072},
             {0x00c4, 0x0042, 0x0308, 0x0043, 0x0308},
             {0x0041, 0x0308, 0x0062, 0x0063},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x0065},
             {0x0072, 0x006f, 0x0302, 0x006c, 0x0065},
             {0x0041, 0x00e1, 0x0063, 0x0064},
             {0x0041, 0x00e1, 0x0063, 0x0064},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0054,
              0x0043,
              0x006f,
              0x006d,
              0x0070,
              0x0061,
              0x0072,
              0x0065,
              0x0050,
              0x006c,
              0x0061,
              0x0069,
              0x006e},
             {0x0061, 0x0042, 0x0063},
             {0x0061, 0x0023, 0x0042},
             {0x0061, 0x0026, 0x0062},
             {0x0061, 0x0023, 0x0063},
             {0x0061, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x00c4, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x00e4, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x00c4, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x00c4, 0x0062, 0x0063, 0x0064, 0x0061},
             {0x0061, 0x0062, 0x0023, 0x0063},
             {0x0061, 0x0062, 0x0063},
             {0x0061, 0x0062, 0x003d, 0x0063},
             {0x0061, 0x0062, 0x0064},
             {0x00e4, 0x0062, 0x0063},
             {0x0061, 0x0043, 0x0048, 0x0063},
             {0x00e4, 0x0062, 0x0063},
             {0x0074, 0x0068, 0x00ee, 0x0073},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x00e9},
             {0x0061, 0x0042, 0x0043},
             {0x0061, 0x0062, 0x0064},
             {0x00e4, 0x0062, 0x0063},
             {0x0061, 0x00c6, 0x0063},
             {0x0061, 0x0042, 0x0064},
             {0x00e4, 0x0062, 0x0063},
             {0x0061, 0x00c6, 0x0063},
             {0x0061, 0x0042, 0x0064},
             {0x00e4, 0x0062, 0x0063},
             {0x0070, 0x00ea, 0x0063, 0x0068, 0x0065}}};

        std::array<int, cases> const result = {
            {-1, -1, -1, 1,  1,  0,  -1, -1, -1, -1, 1,  -1, 0,  -1, 1, 1, 1,
             -1, -1, -1, -1, -1, -1, 1,  1,  1,  -1, -1, 1,  -1, 1,  0, 1, -1,
             -1, -1, 0,  0,  0,  0,  -1, 0,  0,  -1, -1, 0,  -1, -1, -1}};

        for (int i = 38; i < 43; ++i) {
            EXPECT_EQ(
                text::collate(
                    lhs[i].begin(),
                    lhs[i].end(),
                    rhs[i].begin(),
                    rhs[i].end(),
                    table,
                    text::collation_strength::primary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                result[i])
                << "CASE " << i << "\n"
                << lhs[i] << "\n"
                << rhs[i] << "\n";
        }
        for (int i = 43; i < 49; ++i) {
            EXPECT_EQ(
                text::collate(
                    lhs[i].begin(),
                    lhs[i].end(),
                    rhs[i].begin(),
                    rhs[i].end(),
                    table,
                    text::collation_strength::secondary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                result[i])
                << "CASE " << i << "\n"
                << lhs[i] << "\n"
                << rhs[i] << "\n";
        }
        for (int i = 0; i < 38; ++i) {
            EXPECT_EQ(
                text::collate(
                    lhs[i].begin(),
                    lhs[i].end(),
                    rhs[i].begin(),
                    rhs[i].end(),
                    table,
                    text::collation_strength::tertiary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                result[i])
                << "CASE " << i << "\n"
                << lhs[i] << "\n"
                << rhs[i] << "\n";
        }
    }

    {
        int const cases = 10;
        std::array<container::static_vector<uint32_t, 16>, cases> const
            primary_less = {{{0x61},
                             {0x41},
                             {0x65},
                             {0x45},
                             {0x00e9},
                             {0x00e8},
                             {0x00ea},
                             {0x00eb},
                             {0x65, 0x61},
                             {0x78}}};

        for (int i = 0; i < cases; ++i) {
            for (int j = i + 1; j < cases; ++j) {
                EXPECT_EQ(
                    text::collate(
                        primary_less[i].begin(),
                        primary_less[i].end(),
                        primary_less[j].begin(),
                        primary_less[j].end(),
                        table,
                        text::collation_strength::tertiary,
                        text::case_first::off,
                        text::case_level::off,
                        text::variable_weighting::non_ignorable),
                    -1)
                    << "CASE " << i << "\n"
                    << primary_less[i] << "\n"
                    << primary_less[j] << "\n";
            }
        }
    }

    {
        int const cases = 8;
        std::array<container::static_vector<uint32_t, 16>, cases> const
            strings = {{{0x0061, 0x0065},
                        {0x00E6},
                        {0x00C6},
                        {0x0061, 0x0066},
                        {0x006F, 0x0065},
                        {0x0153},
                        {0x0152},
                        {0x006F, 0x0066}}};

        for (int i = 0; i < cases; ++i) {
            for (int j = 0; j < cases; ++j) {
                int expected = 0;
                if (i < j)
                    expected = -1;
                if (j < i)
                    expected = 1;
                EXPECT_EQ(
                    text::collate(
                        strings[i].begin(),
                        strings[i].end(),
                        strings[j].begin(),
                        strings[j].end(),
                        table,
                        text::collation_strength::tertiary,
                        text::case_first::off,
                        text::case_level::off,
                        text::variable_weighting::non_ignorable),
                    expected)
                    << "CASE " << i << "\n"
                    << strings[i] << "\n"
                    << strings[j] << "\n";
            }
        }
    }

    {
        int const cases = 25;
        std::array<container::static_vector<uint32_t, 16>, cases> const
            strings = {{{0x65, 0x65},
                        {0x65, 0x65, 0x0301},
                        {0x65, 0x65, 0x0301, 0x0300},
                        {0x65, 0x65, 0x0300},
                        {0x65, 0x65, 0x0300, 0x0301},
                        {0x65, 0x0301, 0x65},
                        {0x65, 0x0301, 0x65, 0x0301},
                        {0x65, 0x0301, 0x65, 0x0301, 0x0300},
                        {0x65, 0x0301, 0x65, 0x0300},
                        {0x65, 0x0301, 0x65, 0x0300, 0x0301},
                        {0x65, 0x0301, 0x0300, 0x65},
                        {0x65, 0x0301, 0x0300, 0x65, 0x0301},
                        {0x65, 0x0301, 0x0300, 0x65, 0x0301, 0x0300},
                        {0x65, 0x0301, 0x0300, 0x65, 0x0300},
                        {0x65, 0x0301, 0x0300, 0x65, 0x0300, 0x0301},
                        {0x65, 0x0300, 0x65},
                        {0x65, 0x0300, 0x65, 0x0301},
                        {0x65, 0x0300, 0x65, 0x0301, 0x0300},
                        {0x65, 0x0300, 0x65, 0x0300},
                        {0x65, 0x0300, 0x65, 0x0300, 0x0301},
                        {0x65, 0x0300, 0x0301, 0x65},
                        {0x65, 0x0300, 0x0301, 0x65, 0x0301},
                        {0x65, 0x0300, 0x0301, 0x65, 0x0301, 0x0300},
                        {0x65, 0x0300, 0x0301, 0x65, 0x0300},
                        {0x65, 0x0300, 0x0301, 0x65, 0x0300, 0x0301}}};

        for (int i = 0; i < cases; ++i) {
            for (int j = 0; j < cases; ++j) {
                int expected = 0;
                if (i < j)
                    expected = -1;
                if (j < i)
                    expected = 1;
                EXPECT_EQ(
                    text::collate(
                        strings[i].begin(),
                        strings[i].end(),
                        strings[j].begin(),
                        strings[j].end(),
                        table,
                        text::collation_strength::secondary,
                        text::case_first::off,
                        text::case_level::off,
                        text::variable_weighting::non_ignorable),
                    expected)
                    << "CASE " << i << "\n"
                    << strings[i] << "\n"
                    << strings[j] << "\n";
            }
        }
    }
}

TEST(tailoring, es)
{
    text::collation_table const table = text::tailored_collation_table(
        text::data::es::standard_collation_tailoring(),
        "es::standard_collation_tailoring()",
        [](std::string const & s) { std::cout << s; },
        [](std::string const & s) { std::cout << s; });

    int const cases = 9;
    std::array<container::static_vector<uint32_t, 16>, cases> const lhs = {{
        {0x61, 0x6c, 0x69, 0x61, 0x73},
        {0x45, 0x6c, 0x6c, 0x69, 0x6f, 0x74},
        {0x48, 0x65, 0x6c, 0x6c, 0x6f},
        {0x61, 0x63, 0x48, 0x63},
        {0x61, 0x63, 0x63},
        {0x61, 0x6c, 0x69, 0x61, 0x73},
        {0x61, 0x63, 0x48, 0x63},
        {0x61, 0x63, 0x63},
        {0x48, 0x65, 0x6c, 0x6c, 0x6f},
    }};

    std::array<container::static_vector<uint32_t, 16>, cases> const rhs = {{
        {0x61, 0x6c, 0x6c, 0x69, 0x61, 0x73},
        {0x45, 0x6d, 0x69, 0x6f, 0x74},
        {0x68, 0x65, 0x6c, 0x6c, 0x4f},
        {0x61, 0x43, 0x48, 0x63},
        {0x61, 0x43, 0x48, 0x63},
        {0x61, 0x6c, 0x6c, 0x69, 0x61, 0x73},
        {0x61, 0x43, 0x48, 0x63},
        {0x61, 0x43, 0x48, 0x63},
        {0x68, 0x65, 0x6c, 0x6c, 0x4f},
    }};

    std::array<int, cases> const result = {{-1, -1, 1, -1, -1, -1, 0, -1, 0}};

    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(
            text::collate(
                lhs[i].begin(),
                lhs[i].end(),
                rhs[i].begin(),
                rhs[i].end(),
                table,
                text::collation_strength::tertiary,
                text::case_first::off,
                text::case_level::off,
                text::variable_weighting::non_ignorable),
            result[i])
            << "CASE " << i << "\n"
            << lhs[i] << "\n"
            << rhs[i] << "\n";
    }

    for (int i = 5; i < cases; ++i) {
        EXPECT_EQ(
            text::collate(
                lhs[i].begin(),
                lhs[i].end(),
                rhs[i].begin(),
                rhs[i].end(),
                table,
                text::collation_strength::primary,
                text::case_first::off,
                text::case_level::off,
                text::variable_weighting::non_ignorable),
            result[i])
            << "CASE " << i << "\n"
            << lhs[i] << "\n"
            << rhs[i] << "\n";
    }
}

TEST(tailoring, fi)
{
    text::collation_table const table = text::tailored_collation_table(
        text::data::fi::standard_collation_tailoring(),
        "fi::standard_collation_tailoring()",
        [](std::string const & s) { std::cout << s; },
        [](std::string const & s) { std::cout << s; });

    int const cases = 5;
    std::array<container::static_vector<uint32_t, 16>, cases> const lhs = {
        {{0x77, 0x61, 0x74},
         {0x76, 0x61, 0x74},
         {0x61, 0x00FC, 0x62, 0x65, 0x63, 0x6b},
         {0x4c, 0x00E5, 0x76, 0x69},
         {0x77, 0x61, 0x74}}};

    std::array<container::static_vector<uint32_t, 16>, cases> const rhs = {
        {{0x76, 0x61, 0x74},
         {0x77, 0x61, 0x79},
         {0x61, 0x78, 0x62, 0x65, 0x63, 0x6b},
         {0x4c, 0x00E4, 0x77, 0x65},
         {0x76, 0x61, 0x74}}};

    std::array<int, cases> const tertiary_result = {{1, -1, 1, -1, 1}};

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(
            text::collate(
                lhs[i].begin(),
                lhs[i].end(),
                rhs[i].begin(),
                rhs[i].end(),
                table,
                text::collation_strength::tertiary,
                text::case_first::off,
                text::case_level::off,
                text::variable_weighting::non_ignorable),
            tertiary_result[i])
            << "CASE " << i << "\n"
            << lhs[i] << "\n"
            << rhs[i] << "\n";
    }

    EXPECT_EQ(
        text::collate(
            lhs[4].begin(),
            lhs[4].end(),
            rhs[4].begin(),
            rhs[4].end(),
            table,
            text::collation_strength::primary,
            text::case_first::off,
            text::case_level::off,
            text::variable_weighting::non_ignorable),
        tertiary_result[4])
        << "CASE " << 4 << "\n"
        << lhs[4] << "\n"
        << rhs[4] << "\n";
}

TEST(tailoring, fr)
{
    text::collation_table const table = text::tailored_collation_table(
        text::data::fr_CA::standard_collation_tailoring(),
        "fr_CA::standard_collation_tailoring()",
        [](std::string const & s) { std::cout << s; },
        [](std::string const & s) { std::cout << s; });

    {
        int const cases = 12;
        std::array<container::static_vector<uint32_t, 16>, cases> const lhs = {
            {{0x0061, 0x0062, 0x0063},
             {0x0043, 0x004f, 0x0054, 0x0045},
             {0x0063, 0x006f, 0x002d, 0x006f, 0x0070},
             {0x0070, 0x00ea, 0x0063, 0x0068, 0x0065},
             {0x0070, 0x00ea, 0x0063, 0x0068, 0x0065, 0x0072},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x0065, 0x0072},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x0065, 0x0072},
             {0x0048, 0x0065, 0x006c, 0x006c, 0x006f},
             {0x01f1},
             {0xfb00},
             {0x01fa},
             {0x0101}}};

        std::array<container::static_vector<uint32_t, 16>, cases> const rhs = {
            {{0x0041, 0x0042, 0x0043},
             {0x0063, 0x00f4, 0x0074, 0x0065},
             {0x0043, 0x004f, 0x004f, 0x0050},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x00e9},
             {0x0070, 0x00e9, 0x0063, 0x0068, 0x00e9},
             {0x0070, 0x00ea, 0x0063, 0x0068, 0x0065},
             {0x0070, 0x00ea, 0x0063, 0x0068, 0x0065, 0x0072},
             {0x0068, 0x0065, 0x006c, 0x006c, 0x004f},
             {0x01ee},
             {0x25ca},
             {0x00e0},
             {0x01df}}};

        std::array<int, cases> const tertiary_result = {
            {-1, -1, -1, -1, 1, 1, -1, 1, -1, 1, -1, -1}};

        for (int i = 0; i < cases; ++i) {
            EXPECT_EQ(
                text::collate(
                    lhs[i].begin(),
                    lhs[i].end(),
                    rhs[i].begin(),
                    rhs[i].end(),
                    table,
                    text::collation_strength::tertiary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::shifted,
                    text::l2_weight_order::backward),
                tertiary_result[i])
                << "CASE " << i << "\n"
                << lhs[i] << "\n"
                << rhs[i] << "\n";
        }
    }

    {
        int const cases = 10;
        std::array<container::static_vector<uint32_t, 16>, cases> const
            tertiary_less = {{{0x0061},
                              {0x0041},
                              {0x0065},
                              {0x0045},
                              {0x00e9},
                              {0x00e8},
                              {0x00ea},
                              {0x00eb},
                              {0x0065, 0x0061},
                              {0x0078}}};

        for (int i = 0; i < cases - 1; ++i) {
            for (int j = i + 1; j < cases; ++j) {
                EXPECT_EQ(
                    text::collate(
                        tertiary_less[i].begin(),
                        tertiary_less[i].end(),
                        tertiary_less[j].begin(),
                        tertiary_less[j].end(),
                        table,
                        text::collation_strength::tertiary,
                        text::case_first::off,
                        text::case_level::off,
                        text::variable_weighting::non_ignorable),
                    -1)
                    << "CASE " << i << "\n"
                    << tertiary_less[i] << "\n"
                    << tertiary_less[j] << "\n";
            }
        }
    }


    {
        int const cases = 25;
        std::array<container::static_vector<uint32_t, 16>, cases> const
            strings = {{{0x0065, 0x0065},
                        {0x0065, 0x0301, 0x0065},
                        {0x0065, 0x0300, 0x0301, 0x0065},
                        {0x0065, 0x0300, 0x0065},
                        {0x0065, 0x0301, 0x0300, 0x0065},
                        {0x0065, 0x0065, 0x0301},
                        {0x0065, 0x0301, 0x0065, 0x0301},
                        {0x0065, 0x0300, 0x0301, 0x0065, 0x0301},
                        {0x0065, 0x0300, 0x0065, 0x0301},
                        {0x0065, 0x0301, 0x0300, 0x0065, 0x0301},
                        {0x0065, 0x0065, 0x0300, 0x0301},
                        {0x0065, 0x0301, 0x0065, 0x0300, 0x0301},
                        {0x0065, 0x0300, 0x0301, 0x0065, 0x0300, 0x0301},
                        {0x0065, 0x0300, 0x0065, 0x0300, 0x0301},
                        {0x0065, 0x0301, 0x0300, 0x0065, 0x0300, 0x0301},
                        {0x0065, 0x0065, 0x0300},
                        {0x0065, 0x0301, 0x0065, 0x0300},
                        {0x0065, 0x0300, 0x0301, 0x0065, 0x0300},
                        {0x0065, 0x0300, 0x0065, 0x0300},
                        {0x0065, 0x0301, 0x0300, 0x0065, 0x0300},
                        {0x0065, 0x0065, 0x0301, 0x0300},
                        {0x0065, 0x0301, 0x0065, 0x0301, 0x0300},
                        {0x0065, 0x0300, 0x0301, 0x0065, 0x0301, 0x0300},
                        {0x0065, 0x0300, 0x0065, 0x0301, 0x0300},
                        {0x0065, 0x0301, 0x0300, 0x0065, 0x0301, 0x0300}}};

        for (int i = 0; i < cases; ++i) {
            for (int j = 0; j < cases; ++j) {
                int expected = 0;
                if (i < j)
                    expected = -1;
                if (j < i)
                    expected = 1;
                EXPECT_EQ(
                    text::collate(
                        strings[i].begin(),
                        strings[i].end(),
                        strings[j].begin(),
                        strings[j].end(),
                        table,
                        text::collation_strength::secondary,
                        text::case_first::off,
                        text::case_level::off,
                        text::variable_weighting::shifted,
                        text::l2_weight_order::backward),
                    expected)
                    << "CASE " << i << "\n"
                    << strings[i] << "\n"
                    << strings[j] << "\n";
            }
        }
    }
}

TEST(tailoring, ja)
{
    text::collation_table const table = text::tailored_collation_table(
        text::data::ja::standard_collation_tailoring(),
        "ja::standard_collation_tailoring()",
        [](std::string const & s) { std::cout << s; },
        [](std::string const & s) { std::cout << s; });

    {
        int const cases = 6;
        std::array<container::static_vector<uint32_t, 16>, cases> const lhs = {
            {{0xff9e},
             {0x3042},
             {0x30a2},
             {0x3042, 0x3042},
             {0x30a2, 0x30fc},
             {0x30a2, 0x30fc, 0x30c8}}};

        std::array<container::static_vector<uint32_t, 16>, cases> const rhs = {
            {{0xff9f},
             {0x30a2},
             {0x3042, 0x3042},
             {0x30a2, 0x30fc},
             {0x30a2, 0x30fc, 0x30c8},
             {0x3042, 0x3042, 0x3068}}};

        std::array<int, cases> const tertiary_result = {{-1, 0, -1, 1, -1, -1}};

        for (int i = 0; i < cases; ++i) {
            // Differs from ICU behavior.
            if (i == 5)
                continue;
            EXPECT_EQ(
                text::collate(
                    lhs[i].begin(),
                    lhs[i].end(),
                    rhs[i].begin(),
                    rhs[i].end(),
                    table,
                    text::collation_strength::tertiary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                tertiary_result[i])
                << "CASE " << i << "\n"
                << lhs[i] << "\n"
                << rhs[i] << "\n";
        }
    }

    {
        int const cases = 4;
        std::array<container::static_vector<uint32_t, 16>, cases> const
            primary_less = {
                {{0x30ab}, {0x30ab, 0x30ad}, {0x30ad}, {0x30ad, 0x30ad}}};

        for (int i = 0; i < cases - 1; ++i) {
            EXPECT_EQ(
                text::collate(
                    primary_less[i].begin(),
                    primary_less[i].end(),
                    primary_less[i + 1].begin(),
                    primary_less[i + 1].end(),
                    table,
                    text::collation_strength::primary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                -1)
                << "CASE " << i << "\n"
                << primary_less[i] << "\n"
                << primary_less[i + 1] << "\n";
        }
    }

    {
        int const cases = 4;
        std::array<container::static_vector<uint32_t, 16>, cases> const
            secondary_less = {{{0x30cf, 0x30ab},
                               {0x30d0, 0x30ab},
                               {0x30cf, 0x30ad},
                               {0x30d0, 0x30ad}}};

        for (int i = 0; i < cases - 1; ++i) {
            EXPECT_EQ(
                text::collate(
                    secondary_less[i].begin(),
                    secondary_less[i].end(),
                    secondary_less[i + 1].begin(),
                    secondary_less[i + 1].end(),
                    table,
                    text::collation_strength::secondary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                -1)
                << "CASE " << i << "\n"
                << secondary_less[i] << "\n"
                << secondary_less[i + 1] << "\n";
        }
    }

    {
        int const cases = 4;
        std::array<container::static_vector<uint32_t, 16>, cases> const
            tertiary_less = {{{0x30c3, 0x30cf},
                              {0x30c4, 0x30cf},
                              {0x30c3, 0x30d0},
                              {0x30c4, 0x30d0}}};

        for (int i = 0; i < cases - 1; ++i) {
            EXPECT_EQ(
                text::collate(
                    tertiary_less[i].begin(),
                    tertiary_less[i].end(),
                    tertiary_less[i + 1].begin(),
                    tertiary_less[i + 1].end(),
                    table,
                    text::collation_strength::tertiary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                -1)
                << "CASE " << i << "\n"
                << tertiary_less[i] << "\n"
                << tertiary_less[i + 1] << "\n";
        }
    }

    {
        int const cases = 4;
        // Kataga and Hiragana
        std::array<container::static_vector<uint32_t, 16>, cases> const
            quaternary_less = {{{0x3042, 0x30c3},
                                {0x30a2, 0x30c3},
                                {0x3042, 0x30c4},
                                {0x30a2, 0x30c4}}};

        for (int i = 0; i < cases - 1; ++i) {
            EXPECT_EQ(
                text::collate(
                    quaternary_less[i].begin(),
                    quaternary_less[i].end(),
                    quaternary_less[i + 1].begin(),
                    quaternary_less[i + 1].end(),
                    table,
                    text::collation_strength::quaternary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                -1)
                << "CASE " << i << "\n"
                << quaternary_less[i] << "\n"
                << quaternary_less[i + 1] << "\n";
        }
    }

    {
        int const cases = 8;
        // Chooon and Kigoo
        std::array<container::static_vector<uint32_t, 16>, cases> const
            quaternary_less = {{{0x30AB, 0x30FC, 0x3042},
                                {0x30AB, 0x30FC, 0x30A2},
                                {0x30AB, 0x30A4, 0x3042},
                                {0x30AB, 0x30A4, 0x30A2},
                                {0x30AD, 0x30FC, 0x3042},
                                {0x30AD, 0x30FC, 0x30A2},
                                {0x3042, 0x30A4, 0x3042},
                                {0x30A2, 0x30A4, 0x30A2}}};

        for (int i = 0; i < cases - 1; ++i) {
            // Differs from ICU behavior.
            if (i == 3 || i == 5)
                continue;
            EXPECT_EQ(
                text::collate(
                    quaternary_less[i].begin(),
                    quaternary_less[i].end(),
                    quaternary_less[i + 1].begin(),
                    quaternary_less[i + 1].end(),
                    table,
                    text::collation_strength::quaternary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                -1)
                << "CASE " << i << "\n"
                << quaternary_less[i] << "\n"
                << quaternary_less[i + 1] << "\n";
        }
    }
}

TEST(tailoring, th)
{
    text::collation_table const table = text::tailored_collation_table(
        text::data::th::standard_collation_tailoring(),
        "th::standard_collation_tailoring()",
        [](std::string const & s) { std::cout << s; },
        [](std::string const & s) { std::cout << s; });

    std::ifstream ifs("test/riwords.txt", std::ios_base::binary);
    std::vector<std::string> lines;
    while (ifs) {
        std::string line;
        char c;
        while (ifs.get(c)) {
            if (c != '\r')
                line += c;
            if (c == '\n')
                break;
        }
        if (line.empty() || line[0] == '#')
            continue;
        lines.push_back(line);
    }

    for (int i = 0, end = int(lines.size()) - 1; i < end; ++i) {
        auto const i_ = text::as_utf32(lines[i]);
        auto const i_1 = text::as_utf32(lines[i + 1]);
        auto const collation = text::collate(
            i_.begin(),
            i_.end(),
            i_1.begin(),
            i_1.end(),
            table,
            text::collation_strength::tertiary);
        EXPECT_LE(collation, 0) << "i=" << i;
        if (0 < collation) {
            for (auto cp : i_) {
                std::cout << "0x" << std::hex << cp << " ";
            }
            std::cout << "\n";

            std::cout << text::collation_sort_key(
                             i_.begin(),
                             i_.end(),
                             table,
                             text::collation_strength::tertiary)
                      << "\n";

            for (auto cp : i_1) {
                std::cout << "0x" << std::hex << cp << " ";
            }
            std::cout << "\n";

            std::cout << text::collation_sort_key(
                             i_1.begin(),
                             i_1.end(),
                             table,
                             text::collation_strength::tertiary)
                      << "\n";

            std::cout << "\n";
        }
    }

    {
        int const cases = 13;
        std::array<text::string_view, cases> const lhs = {
            {(char const *)u8"\u0e01",
             (char const *)u8"\u0e01\u0e32",
             (char const *)u8"\u0e01\u0e32",
             (char const *)u8"\u0e01\u0e32\u0e01\u0e49\u0e32",
             (char const *)u8"\u0e01\u0e32",
             (char const *)u8"\u0e01\u0e32-",
             (char const *)u8"\u0e01\u0e32",
             (char const *)u8"\u0e01\u0e32\u0e46",
             (char const *)u8"\u0e24\u0e29\u0e35",
             (char const *)u8"\u0e26\u0e29\u0e35",
             (char const *)u8"\u0e40\u0e01\u0e2d",
             (char const *)u8"\u0e01\u0e32\u0e01\u0e48\u0e32",
             (char const *)u8"\u0e01.\u0e01."}};

        std::array<text::string_view, cases> const rhs = {
            {(char const *)u8"\u0e01\u0e01",
             (char const *)u8"\u0e01\u0e49\u0e32",
             (char const *)u8"\u0e01\u0e32\u0e4c",
             (char const *)u8"\u0e01\u0e48\u0e32\u0e01\u0e49\u0e32",
             (char const *)u8"\u0e01\u0e32-",
             (char const *)u8"\u0e01\u0e32\u0e01\u0e32",
             (char const *)u8"\u0e01\u0e32\u0e46",
             (char const *)u8"\u0e01\u0e32\u0e01\u0e32",
             (char const *)u8"\u0e24\u0e45\u0e29\u0e35",
             (char const *)u8"\u0e26\u0e45\u0e29\u0e35",
             (char const *)u8"\u0e40\u0e01\u0e34",
             (char const *)u8"\u0e01\u0e49\u0e32\u0e01\u0e32",
             (char const *)u8"\u0e01\u0e32"}};

        std::array<int, cases> const tertiary_result = {
            {-1, -1, -1, -1, 0, -1, 0, -1, -1, -1, -1, -1, -1}};

        for (int i = 0; i < cases; ++i) {
            auto const lhs_ = text::as_utf32(lhs[i]);
            auto const rhs_ = text::as_utf32(rhs[i]);
            EXPECT_EQ(
                text::collate(
                    lhs_.begin(),
                    lhs_.end(),
                    rhs_.begin(),
                    rhs_.end(),
                    table,
                    text::collation_strength::tertiary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                tertiary_result[i])
                << "CASE " << i << "\n"
                << text::as_utf32(lhs[i]) << "\n"
                << text::as_utf32(rhs[i]) << "\n";
        }
    }

    {
        int const cases = 26;
        std::array<text::string_view, cases> const lhs = {
            {(char const *)u8"\u0E41c\u0301",          (char const *)u8"\u0E41\U0001D7CE",
             (char const *)u8"\u0E41\U0001D15F",       (char const *)u8"\u0E41\U0002F802",
             (char const *)u8"\u0E41\u0301",           (char const *)u8"\u0E41\u0301\u0316",
             (char const *)u8"\u0e24\u0e41",           (char const *)u8"\u0e3f\u0e3f\u0e24\u0e41",
             (char const *)u8"abc\u0E41c\u0301",       (char const *)u8"abc\u0E41\U0001D000",
             (char const *)u8"abc\u0E41\U0001D15F",    (char const *)u8"abc\u0E41\U0002F802",
             (char const *)u8"abc\u0E41\u0301",        (char const *)u8"abc\u0E41\u0301\u0316",
             (char const *)u8"\u0E41c\u0301abc",       (char const *)u8"\u0E41\U0001D000abc",
             (char const *)u8"\u0E41\U0001D15Fabc",    (char const *)u8"\u0E41\U0002F802abc",
             (char const *)u8"\u0E41\u0301abc",        (char const *)u8"\u0E41\u0301\u0316abc",
             (char const *)u8"abc\u0E41c\u0301abc",    (char const *)u8"abc\u0E41\U0001D000abc",
             (char const *)u8"abc\u0E41\U0001D15Fabc", (char const *)u8"abc\u0E41\U0002F802abc",
             (char const *)u8"abc\u0E41\u0301abc",     (char const *)u8"abc\u0E41\u0301\u0316abc"}};

        std::array<text::string_view, cases> const rhs = {
            {(char const *)u8"\u0E41\u0107",
             (char const *)u8"\u0E41\U0001D7CF",
             (char const *)u8"\u0E41\U0001D158\U0001D165",
             (char const *)u8"\u0E41\u4E41",
             (char const *)u8"\u0E41\u0301",
             (char const *)u8"\u0E41\u0316\u0301",
             (char const *)u8"\u0e41\u0e24",
             (char const *)u8"\u0e3f\u0e3f\u0e41\u0e24",
             (char const *)u8"abc\u0E41\u0107",
             (char const *)u8"abc\u0E41\U0001D001",
             (char const *)u8"abc\u0E41\U0001D158\U0001D165",
             (char const *)u8"abc\u0E41\u4E41",
             (char const *)u8"abc\u0E41\u0301",
             (char const *)u8"abc\u0E41\u0316\u0301",
             (char const *)u8"\u0E41\u0107abc",
             (char const *)u8"\u0E41\U0001D001abc",
             (char const *)u8"\u0E41\U0001D158\U0001D165abc",
             (char const *)u8"\u0E41\u4E41abc",
             (char const *)u8"\u0E41\u0301abc",
             (char const *)u8"\u0E41\u0316\u0301abc",
             (char const *)u8"abc\u0E41\u0107abc",
             (char const *)u8"abc\u0E41\U0001D001abc",
             (char const *)u8"abc\u0E41\U0001D158\U0001D165abc",
             (char const *)u8"abc\u0E41\u4E41abc",
             (char const *)u8"abc\u0E41\u0301abc",
             (char const *)u8"abc\u0E41\u0316\u0301abc"}};

        // Differs from ICU behavior; changed cases 5, 13, 19, and 25 from 0
        // to -1, since they have secondary differences.  Need to understand
        // if this is the right thing to do.
        std::array<int, cases> const secondary_result = {
            {0,  -1, 0,  0, 0, -1, 0,  0, 0,  -1, 0, 0, 0,
             -1, 0,  -1, 0, 0, 0,  -1, 0, -1, 0,  0, 0, -1}};

        for (int i = 0; i < cases; ++i) {
            auto const lhs_ = text::as_utf32(lhs[i]);
            auto const rhs_ = text::as_utf32(rhs[i]);
            EXPECT_EQ(
                text::collate(
                    lhs_.begin(),
                    lhs_.end(),
                    rhs_.begin(),
                    rhs_.end(),
                    table,
                    text::collation_strength::secondary,
                    text::case_first::off,
                    text::case_level::off,
                    text::variable_weighting::non_ignorable),
                secondary_result[i])
                << "CASE " << i << "\n"
                << text::as_utf32(lhs[i]) << "\n"
                << text::as_utf32(rhs[i]) << "\n";
        }
    }

    {
        text::collation_table const custom_table =
            text::tailored_collation_table(
                "& c < ab",
                "custom-tailoring",
                [](std::string const & s) { std::cout << s; },
                [](std::string const & s) { std::cout << s; });

        std::string const a("\u0e41ab");
        std::string const b("\u0e41c");
        auto const a_ = text::as_utf32(a);
        auto const b_ = text::as_utf32(b);
        EXPECT_EQ(
            text::collate(
                a_.begin(),
                a_.end(),
                b_.begin(),
                b_.end(),
                custom_table,
                text::collation_strength::tertiary,
                text::case_first::off,
                text::case_level::off,
                text::variable_weighting::non_ignorable),
            1);
    }
}

TEST(tailoring, tr)
{
    text::collation_table const table = text::tailored_collation_table(
        text::data::tr::standard_collation_tailoring(),
        "tr::standard_collation_tailoring()",
        [](std::string const & s) { std::cout << s; },
        [](std::string const & s) { std::cout << s; });

    int const cases = 11;
    std::array<container::static_vector<uint32_t, 16>, cases> const lhs = {
        {{0x73, 0x0327},
         {0x76, 0x00e4, 0x74},
         {0x6f, 0x6c, 0x64},
         {0x00fc, 0x6f, 0x69, 0x64},
         {0x68, 0x011e, 0x61, 0x6c, 0x74},
         {0x73, 0x74, 0x72, 0x65, 0x73, 0x015e},
         {0x76, 0x6f, 0x0131, 0x64},
         {0x69, 0x64, 0x65, 0x61},
         {0x00fc, 0x6f, 0x69, 0x64},
         {0x76, 0x6f, 0x0131, 0x64},
         {0x69, 0x64, 0x65, 0x61}}};

    std::array<container::static_vector<uint32_t, 16>, cases> const rhs = {
        {{0x75, 0x0308},
         {0x76, 0x62, 0x74},
         {0x00d6, 0x61, 0x79},
         {0x76, 0x6f, 0x69, 0x64},
         {0x68, 0x61, 0x6c, 0x74},
         {0x015e, 0x74, 0x72, 0x65, 0x015e, 0x73},
         {0x76, 0x6f, 0x69, 0x64},
         {0x49, 0x64, 0x65, 0x61},
         {0x76, 0x6f, 0x69, 0x64},
         {0x76, 0x6f, 0x69, 0x64},
         {0x49, 0x64, 0x65, 0x61}}};

    std::array<int, cases> const tertiary_result = {
        {-1, -1, -1, -1, 1, -1, -1, 1, -1, -1, 1}};

    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(
            text::collate(
                lhs[i].begin(),
                lhs[i].end(),
                rhs[i].begin(),
                rhs[i].end(),
                table,
                text::collation_strength::tertiary,
                text::case_first::off,
                text::case_level::off,
                text::variable_weighting::non_ignorable),
            tertiary_result[i])
            << "CASE " << i << "\n"
            << lhs[i] << "\n"
            << rhs[i] << "\n";
    }
    for (int i = 8; i < cases; ++i) {
        EXPECT_EQ(
            text::collate(
                lhs[i].begin(),
                lhs[i].end(),
                rhs[i].begin(),
                rhs[i].end(),
                table,
                text::collation_strength::primary,
                text::case_first::off,
                text::case_level::off,
                text::variable_weighting::non_ignorable),
            tertiary_result[i])
            << "CASE " << i << "\n"
            << lhs[i] << "\n"
            << rhs[i] << "\n";
    }
}
