// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#include <boost/text/collation_table.hpp>
#include <boost/text/collate.hpp>
#include <boost/text/data/all.hpp>

#ifndef LIMIT_TESTING_FOR_CI
#include <boost/text/save_load_table.hpp>

#include <boost/filesystem.hpp>
#endif

#include <gtest/gtest.h>

using namespace boost::text;

auto const error = [](std::string const & s) { std::cout << s; };
auto const warning = [](std::string const & s) {};

collation_table make_save_load_table()
{
#ifdef LIMIT_TESTING_FOR_CI
    std::string const table_str(data::es::traditional_collation_tailoring());
    return tailored_collation_table(
        table_str,
        "es::traditional_collation_tailoring()", error, warning);
#else
    if (!exists(boost::filesystem::path("es_traditional.table"))) {
        std::string const table_str(data::es::traditional_collation_tailoring());
        collation_table table = tailored_collation_table(
            table_str,
            "es::traditional_collation_tailoring()", error, warning);
        save_table(table, "es_traditional.table.0");
        boost::filesystem::rename("es_traditional.table.0", "es_traditional.table");
    }
    return load_table("es_traditional.table");
#endif
}
collation_table const & table()
{
    static collation_table retval = make_save_load_table();
    return retval;
}
TEST(tailoring, es_traditional_000_001)
{
    {
    // greater than (or equal to, for =) preceeding cps
    auto const res = std::vector<uint32_t>(1, 0x004e);
    auto const rel = std::vector<uint32_t>{0x006e, 0x0303};
    std::string const res_str = to_string(res);
    std::string const rel_str = to_string(rel);
    auto const res_view = as_utf32(res);
    auto const rel_view = as_utf32(rel);
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::primary),
        -1);
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::primary),
        -1);
    }
    {
    // greater than (or equal to, for =) preceeding cps
    auto const res = std::vector<uint32_t>{0x006e, 0x0303};
    auto const rel = std::vector<uint32_t>{0x004e, 0x0303};
    std::string const res_str = to_string(res);
    std::string const rel_str = to_string(rel);
    auto const res_view = as_utf32(res);
    auto const rel_view = as_utf32(rel);
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::tertiary),
        -1);
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::tertiary),
        -1);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::secondary),
        0);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::secondary),
        0);
    }
    {
    // greater than (or equal to, for =) preceeding cps
    auto const res = std::vector<uint32_t>(1, 0x0043);
    auto const rel = std::vector<uint32_t>{0x0063, 0x0068};
    std::string const res_str = to_string(res);
    std::string const rel_str = to_string(rel);
    auto const res_view = as_utf32(res);
    auto const rel_view = as_utf32(rel);
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::primary),
        -1);
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::primary),
        -1);
    }
    {
    // greater than (or equal to, for =) preceeding cps
    auto const res = std::vector<uint32_t>{0x0063, 0x0068};
    auto const rel = std::vector<uint32_t>{0x0043, 0x0068};
    std::string const res_str = to_string(res);
    std::string const rel_str = to_string(rel);
    auto const res_view = as_utf32(res);
    auto const rel_view = as_utf32(rel);
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::tertiary),
        -1);
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::tertiary),
        -1);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::secondary),
        0);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::secondary),
        0);
    }
    {
    // greater than (or equal to, for =) preceeding cps
    auto const res = std::vector<uint32_t>{0x0043, 0x0068};
    auto const rel = std::vector<uint32_t>{0x0043, 0x0048};
    std::string const res_str = to_string(res);
    std::string const rel_str = to_string(rel);
    auto const res_view = as_utf32(res);
    auto const rel_view = as_utf32(rel);
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::tertiary),
        -1);
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::tertiary),
        -1);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::secondary),
        0);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::secondary),
        0);
    }
    {
    // greater than (or equal to, for =) preceeding cps
    auto const res = std::vector<uint32_t>(1, 0x006c);
    auto const rel = std::vector<uint32_t>{0x006c, 0x006c};
    std::string const res_str = to_string(res);
    std::string const rel_str = to_string(rel);
    auto const res_view = as_utf32(res);
    auto const rel_view = as_utf32(rel);
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::primary),
        -1);
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::primary),
        -1);
    }
    {
    // greater than (or equal to, for =) preceeding cps
    auto const res = std::vector<uint32_t>{0x006c, 0x006c};
    auto const rel = std::vector<uint32_t>{0x004c, 0x006c};
    std::string const res_str = to_string(res);
    std::string const rel_str = to_string(rel);
    auto const res_view = as_utf32(res);
    auto const rel_view = as_utf32(rel);
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::tertiary),
        -1);
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::tertiary),
        -1);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::secondary),
        0);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::secondary),
        0);
    }
    {
    // greater than (or equal to, for =) preceeding cps
    auto const res = std::vector<uint32_t>{0x004c, 0x006c};
    auto const rel = std::vector<uint32_t>{0x004c, 0x004c};
    std::string const res_str = to_string(res);
    std::string const rel_str = to_string(rel);
    auto const res_view = as_utf32(res);
    auto const rel_view = as_utf32(rel);
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::tertiary),
        -1);
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::tertiary),
        -1);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res.begin(), res.end(),
        rel.begin(), rel.end(),
        table(), collation_strength::secondary),
        0);
    // equal to preceeding cps at next-lower strength
    EXPECT_EQ(collate(
        res_view.begin(), res_view.end(),
        rel_view.begin(), rel_view.end(),
        table(), collation_strength::secondary),
        0);
    }
}
