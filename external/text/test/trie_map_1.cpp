// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
#define ENABLE_DUMP 0
#include "trie_tests.hpp"

#include <boost/text/trie_map.hpp>

#include <gtest/gtest.h>

#include <array>


using namespace boost;

TEST(trie_map1, const_access)
{
    {
        boost::text::trie_map<std::vector<int>, int> const trie(
            {{{0, 1, 3}, 13}, {{0}, 17}, {{0, 1, 2}, 19}});

        EXPECT_FALSE(trie.empty());
        EXPECT_EQ(trie.size(), 3u);
        EXPECT_EQ(trie.max_size(), PTRDIFF_MAX);

        {
            std::vector<text::trie_element<std::vector<int>, int>> const
                expected_elements = {
                    {{0}, 17}, {{0, 1, 2}, 19}, {{0, 1, 3}, 13}};
            std::vector<text::trie_element<std::vector<int>, int>>
                copied_elements(trie.size());

            std::copy(trie.begin(), trie.end(), copied_elements.begin());
            EXPECT_EQ(copied_elements, expected_elements);

            std::copy(trie.rbegin(), trie.rend(), copied_elements.rbegin());
            EXPECT_EQ(copied_elements, expected_elements);
        }
    }

    {
        boost::text::trie_map<std::string, int> const trie({{"", 42}});
        auto const _it = trie.begin();
        EXPECT_EQ(_it->key, "");

        auto const match = trie.longest_subsequence("whatever");
        EXPECT_TRUE(match.node != nullptr);
        EXPECT_EQ(match.size, 0);
        EXPECT_EQ(match.match, true);
    }

    {
        boost::text::trie_map<std::wstring, int> const trie({{L"", 42}});
        auto const _it = trie.begin();
        EXPECT_EQ(_it->key, L"");

        auto const match = trie.longest_subsequence(L"whatever");
        EXPECT_TRUE(match.node != nullptr);
        EXPECT_EQ(match.size, 0);
        EXPECT_EQ(match.match, true);
    }

    {
        boost::text::trie_map<std::string, int> const trie({{"w", 42}});
        auto const _it = trie.begin();
        EXPECT_EQ(_it->key, "w");

        auto const match = trie.longest_subsequence("whatever");
        EXPECT_TRUE(match.node != nullptr);
        EXPECT_EQ(match.size, 1);
        EXPECT_EQ(match.match, true);
    }

    {
        boost::text::trie_map<std::string, int> const trie(
            {{"foo", 13}, {"bar", 17}, {"fool", 19}, {"foon", 19}, {"", 42}});

#if ENABLE_DUMP
        // dump(std::cout, trie);
#endif

        EXPECT_EQ(trie.size(), 5u);

        {
            EXPECT_TRUE(trie.contains(std::string("foo")));

            EXPECT_TRUE(trie.contains("foo"));

            std::array<char, 3> foo_array = {{'f', 'o', 'o'}};
            EXPECT_TRUE(trie.contains(foo_array));

            std::vector<char> foo_vec = {'f', 'o', 'o'};
            EXPECT_TRUE(trie.contains(foo_vec));

            std::string foo_str = "foo";
            EXPECT_TRUE(trie.contains(foo_str));
        }

        {
            EXPECT_FALSE(trie.contains(std::string("baz")));

            EXPECT_FALSE(trie.contains("baz"));

            std::array<char, 3> baz_array = {{'b', 'a', 'z'}};
            EXPECT_FALSE(trie.contains(baz_array));

            std::vector<char> baz_vec = {'b', 'a', 'z'};
            EXPECT_FALSE(trie.contains(baz_vec));

            std::string baz_str = "baz";
            EXPECT_FALSE(trie.contains(baz_str));
        }

        {
            auto const _it = trie.begin();
            auto const bar_it = std::next(_it);
            auto const foo_it = std::next(bar_it);
            auto const fool_it = std::next(foo_it);
            auto const foon_it = std::next(fool_it);
            auto const end = trie.end();

            EXPECT_EQ(trie.find(""), _it);

            EXPECT_EQ(trie.find("X"), end);
            EXPECT_EQ(trie.find("b"), end);
            EXPECT_EQ(trie.find("ba"), end);
            EXPECT_EQ(trie.find("bar"), bar_it);
            EXPECT_EQ(trie.find("bart"), end);

            EXPECT_EQ(trie.find("f"), end);
            EXPECT_EQ(trie.find("fo"), end);
            EXPECT_EQ(trie.find("foo"), foo_it);
            EXPECT_EQ(trie.find("fook"), end);
            EXPECT_EQ(trie.find("fool"), fool_it);
            EXPECT_EQ(trie.find("foom"), end);
            EXPECT_EQ(trie.find("fooms"), end);
            EXPECT_EQ(trie.find("foon"), foon_it);
            EXPECT_EQ(trie.find("fooo"), end);
            EXPECT_EQ(trie.find("foons"), end);
        }

        {
            auto const _it = trie.begin();
            auto const bar_it = std::next(_it);
            auto const foo_it = std::next(bar_it);
            auto const fool_it = std::next(foo_it);
            auto const foon_it = std::next(fool_it);
            auto const end = trie.end();

            EXPECT_EQ(trie.lower_bound(""), _it);

            EXPECT_EQ(trie.lower_bound("X"), bar_it);
            EXPECT_EQ(trie.lower_bound("b"), bar_it);
            EXPECT_EQ(trie.lower_bound("ba"), bar_it);
            EXPECT_EQ(trie.lower_bound("bar"), bar_it);
            EXPECT_EQ(trie.lower_bound("bart"), foo_it);

            EXPECT_EQ(trie.lower_bound("f"), foo_it);
            EXPECT_EQ(trie.lower_bound("fo"), foo_it);
            EXPECT_EQ(trie.lower_bound("foo"), foo_it);
            EXPECT_EQ(trie.lower_bound("fook"), fool_it);
            EXPECT_EQ(trie.lower_bound("fool"), fool_it);
            EXPECT_EQ(trie.lower_bound("foom"), foon_it);
            EXPECT_EQ(trie.lower_bound("fooms"), foon_it);
            EXPECT_EQ(trie.lower_bound("foon"), foon_it);
            EXPECT_EQ(trie.lower_bound("fooo"), end);
            EXPECT_EQ(trie.lower_bound("foons"), end);
        }

        {
            auto const _it = trie.begin();
            auto const bar_it = std::next(_it);
            auto const foo_it = std::next(bar_it);
            auto const fool_it = std::next(foo_it);
            auto const foon_it = std::next(fool_it);
            auto const end = trie.end();

            EXPECT_EQ(trie.upper_bound(""), bar_it);

            EXPECT_EQ(trie.upper_bound("X"), bar_it);
            EXPECT_EQ(trie.upper_bound("b"), bar_it);
            EXPECT_EQ(trie.upper_bound("ba"), bar_it);
            EXPECT_EQ(trie.upper_bound("bar"), foo_it);
            EXPECT_EQ(trie.upper_bound("bart"), foo_it);

            EXPECT_EQ(trie.upper_bound("f"), foo_it);
            EXPECT_EQ(trie.upper_bound("fo"), foo_it);
            EXPECT_EQ(trie.upper_bound("foo"), fool_it);
            EXPECT_EQ(trie.upper_bound("fook"), fool_it);
            EXPECT_EQ(trie.upper_bound("fool"), foon_it);
            EXPECT_EQ(trie.upper_bound("foom"), foon_it);
            EXPECT_EQ(trie.upper_bound("fooms"), foon_it);
            EXPECT_EQ(trie.upper_bound("foon"), end);
            EXPECT_EQ(trie.upper_bound("fooo"), end);
            EXPECT_EQ(trie.upper_bound("foons"), end);
        }

        {
            EXPECT_EQ(*trie[""], 42);
        }

        {
            auto const _match = trie.longest_subsequence("");
            EXPECT_TRUE(_match.node != nullptr);
            EXPECT_EQ(_match.size, 0);
            EXPECT_EQ(_match.match, true);

            auto const _0_match = trie.extend_subsequence(_match, 0);
            EXPECT_EQ(_0_match, _match);

            auto const f_match = trie.extend_subsequence(_match, 'f');
            EXPECT_TRUE(f_match.node != nullptr);
            EXPECT_EQ(f_match.size, 1);
            EXPECT_EQ(f_match.match, false);
        }

        {
            auto const fo_match = trie.longest_subsequence("fo");
            EXPECT_TRUE(fo_match.node != nullptr);
            EXPECT_EQ(fo_match.size, 2);
            EXPECT_EQ(fo_match.match, false);
        }

        {
            auto const fa_match = trie.longest_subsequence("fa");
            EXPECT_TRUE(fa_match.node != nullptr);
            EXPECT_EQ(fa_match.size, 1);
            EXPECT_EQ(fa_match.match, false);
        }

        {
            auto const bart_match = trie.longest_subsequence("bart");
            EXPECT_TRUE(bart_match.node != nullptr);
            EXPECT_EQ(bart_match.size, 3);
            EXPECT_EQ(bart_match.match, true);
        }
    }
}

TEST(trie_map1, mutable_access)
{
    {
        boost::text::trie_map<std::vector<int>, int> trie(
            {{{0, 1, 3}, 13}, {{0}, 17}, {{0, 1, 2}, 19}});

        EXPECT_FALSE(trie.empty());
        EXPECT_EQ(trie.size(), 3u);
        EXPECT_EQ(trie.max_size(), PTRDIFF_MAX);

        {
            std::vector<text::trie_element<std::vector<int>, int>> const
                expected_elements = {
                    {{0}, 17}, {{0, 1, 2}, 19}, {{0, 1, 3}, 13}};
            std::vector<text::trie_element<std::vector<int>, int>>
                copied_elements(trie.size());

            std::copy(trie.begin(), trie.end(), copied_elements.begin());
            EXPECT_EQ(copied_elements, expected_elements);

            std::copy(trie.rbegin(), trie.rend(), copied_elements.rbegin());
            EXPECT_EQ(copied_elements, expected_elements);
        }
    }

    {
        boost::text::trie_map<std::string, int> trie({{"", 42}});
        auto const _it = trie.begin();
        EXPECT_EQ(_it->key, "");

        auto const match = trie.longest_subsequence("whatever");
        EXPECT_TRUE(match.node != nullptr);
        EXPECT_EQ(match.size, 0);
        EXPECT_EQ(match.match, true);
    }

    {
        boost::text::trie_map<std::string, int> trie({{"w", 42}});
        auto const _it = trie.begin();
        EXPECT_EQ(_it->key, "w");

        auto const match = trie.longest_subsequence("whatever");
        EXPECT_TRUE(match.node != nullptr);
        EXPECT_EQ(match.size, 1);
        EXPECT_EQ(match.match, true);
    }

    {
        boost::text::trie_map<std::string, int> trie(
            {{"foo", 13}, {"bar", 17}, {"fool", 19}, {"foon", 19}, {"", 42}});

#if ENABLE_DUMP
        // dump(std::cout, trie);
#endif

        EXPECT_EQ(trie.size(), 5u);

        {
            EXPECT_TRUE(trie.contains(std::string("foo")));

            EXPECT_TRUE(trie.contains("foo"));

            std::array<char, 3> foo_array = {{'f', 'o', 'o'}};
            EXPECT_TRUE(trie.contains(foo_array));

            std::vector<char> foo_vec = {'f', 'o', 'o'};
            EXPECT_TRUE(trie.contains(foo_vec));

            std::string foo_str = "foo";
            EXPECT_TRUE(trie.contains(foo_str));
        }

        {
            EXPECT_FALSE(trie.contains(std::string("baz")));

            EXPECT_FALSE(trie.contains("baz"));

            std::array<char, 3> baz_array = {{'b', 'a', 'z'}};
            EXPECT_FALSE(trie.contains(baz_array));

            std::vector<char> baz_vec = {'b', 'a', 'z'};
            EXPECT_FALSE(trie.contains(baz_vec));

            std::string baz_str = "baz";
            EXPECT_FALSE(trie.contains(baz_str));
        }

        {
            auto const _it = trie.begin();
            auto const bar_it = std::next(_it);
            auto const foo_it = std::next(bar_it);
            auto const fool_it = std::next(foo_it);
            auto const foon_it = std::next(fool_it);
            auto const end = trie.end();

            EXPECT_EQ(trie.find(""), _it);

            EXPECT_EQ(trie.find("X"), end);
            EXPECT_EQ(trie.find("b"), end);
            EXPECT_EQ(trie.find("ba"), end);
            EXPECT_EQ(trie.find("bar"), bar_it);
            EXPECT_EQ(trie.find("bart"), end);

            EXPECT_EQ(trie.find("f"), end);
            EXPECT_EQ(trie.find("fo"), end);
            EXPECT_EQ(trie.find("foo"), foo_it);
            EXPECT_EQ(trie.find("fook"), end);
            EXPECT_EQ(trie.find("fool"), fool_it);
            EXPECT_EQ(trie.find("foom"), end);
            EXPECT_EQ(trie.find("fooms"), end);
            EXPECT_EQ(trie.find("foon"), foon_it);
            EXPECT_EQ(trie.find("fooo"), end);
            EXPECT_EQ(trie.find("foons"), end);
        }

        {
            auto const _it = trie.begin();
            auto const bar_it = std::next(_it);
            auto const foo_it = std::next(bar_it);
            auto const fool_it = std::next(foo_it);
            auto const foon_it = std::next(fool_it);
            auto const end = trie.end();

            EXPECT_EQ(trie.lower_bound(""), _it);

            EXPECT_EQ(trie.lower_bound("X"), bar_it);
            EXPECT_EQ(trie.lower_bound("b"), bar_it);
            EXPECT_EQ(trie.lower_bound("ba"), bar_it);
            EXPECT_EQ(trie.lower_bound("bar"), bar_it);
            EXPECT_EQ(trie.lower_bound("bart"), foo_it);

            EXPECT_EQ(trie.lower_bound("f"), foo_it);
            EXPECT_EQ(trie.lower_bound("fo"), foo_it);
            EXPECT_EQ(trie.lower_bound("foo"), foo_it);
            EXPECT_EQ(trie.lower_bound("fook"), fool_it);
            EXPECT_EQ(trie.lower_bound("fool"), fool_it);
            EXPECT_EQ(trie.lower_bound("foom"), foon_it);
            EXPECT_EQ(trie.lower_bound("fooms"), foon_it);
            EXPECT_EQ(trie.lower_bound("foon"), foon_it);
            EXPECT_EQ(trie.lower_bound("fooo"), end);
            EXPECT_EQ(trie.lower_bound("foons"), end);
        }

        {
            auto const _it = trie.begin();
            auto const bar_it = std::next(_it);
            auto const foo_it = std::next(bar_it);
            auto const fool_it = std::next(foo_it);
            auto const foon_it = std::next(fool_it);
            auto const end = trie.end();

            EXPECT_EQ(trie.upper_bound(""), bar_it);

            EXPECT_EQ(trie.upper_bound("X"), bar_it);
            EXPECT_EQ(trie.upper_bound("b"), bar_it);
            EXPECT_EQ(trie.upper_bound("ba"), bar_it);
            EXPECT_EQ(trie.upper_bound("bar"), foo_it);
            EXPECT_EQ(trie.upper_bound("bart"), foo_it);

            EXPECT_EQ(trie.upper_bound("f"), foo_it);
            EXPECT_EQ(trie.upper_bound("fo"), foo_it);
            EXPECT_EQ(trie.upper_bound("foo"), fool_it);
            EXPECT_EQ(trie.upper_bound("fook"), fool_it);
            EXPECT_EQ(trie.upper_bound("fool"), foon_it);
            EXPECT_EQ(trie.upper_bound("foom"), foon_it);
            EXPECT_EQ(trie.upper_bound("fooms"), foon_it);
            EXPECT_EQ(trie.upper_bound("foon"), end);
            EXPECT_EQ(trie.upper_bound("fooo"), end);
            EXPECT_EQ(trie.upper_bound("foons"), end);
        }

        {
            EXPECT_EQ(*trie[""], 42);
        }

        {
            auto const _match = trie.longest_subsequence("");
            EXPECT_TRUE(_match.node != nullptr);
            EXPECT_EQ(_match.size, 0);
            EXPECT_EQ(_match.match, true);

            auto const _0_match = trie.extend_subsequence(_match, 0);
            EXPECT_EQ(_0_match, _match);

            auto const f_match = trie.extend_subsequence(_match, 'f');
            EXPECT_TRUE(f_match.node != nullptr);
            EXPECT_EQ(f_match.size, 1);
            EXPECT_EQ(f_match.match, false);
        }

        {
            auto const fo_match = trie.longest_subsequence("fo");
            EXPECT_TRUE(fo_match.node != nullptr);
            EXPECT_EQ(fo_match.size, 2);
            EXPECT_EQ(fo_match.match, false);
        }

        {
            auto const fa_match = trie.longest_subsequence("fa");
            EXPECT_TRUE(fa_match.node != nullptr);
            EXPECT_EQ(fa_match.size, 1);
            EXPECT_EQ(fa_match.match, false);
        }

        {
            auto const bart_match = trie.longest_subsequence("bart");
            EXPECT_TRUE(bart_match.node != nullptr);
            EXPECT_EQ(bart_match.size, 3);
            EXPECT_EQ(bart_match.match, true);
        }
    }
}

TEST(trie_map, index_operator)
{
    {
        boost::text::trie_map<std::string, int> trie(
            {{"foo", 13}, {"bar", 17}, {"foos", 19}, {"", 0}});

        EXPECT_TRUE(trie["foo"]);
        EXPECT_TRUE(trie["bar"]);
        EXPECT_TRUE(trie["foos"]);
        EXPECT_TRUE(trie[""]);
        EXPECT_FALSE(trie["other"]);

        // boost::optional/std::optional style
        EXPECT_EQ(*trie["foo"], 13);
        EXPECT_EQ(*trie["bar"], 17);
        EXPECT_EQ(*trie["foos"], 19);
        EXPECT_EQ(*trie[""], 0);

        *trie["foo"] = 0;
        *trie["bar"] = 1;
        *trie["foos"] = 2;
        *trie[""] = 3;

        EXPECT_EQ(*trie["foo"], 0);
        EXPECT_EQ(*trie["bar"], 1);
        EXPECT_EQ(*trie["foos"], 2);
        EXPECT_EQ(*trie[""], 3);

        // new style
        trie["foo"] = 3;
        trie["bar"] = 2;
        trie["foos"] = 1;
        trie[""] = 0;

        EXPECT_EQ(trie["foo"], 3);
        EXPECT_EQ(trie["bar"], 2);
        EXPECT_EQ(trie["foos"], 1);
        EXPECT_EQ(trie[""], 0);

        auto foo_ref = trie["foo"];
        auto bar_ref = trie["bar"];
        foo_ref = bar_ref;
        EXPECT_EQ(foo_ref, 2);
        EXPECT_EQ(bar_ref, 2);
    }

    {
        boost::text::trie_map<std::string, int> const trie(
            {{"foo", 13}, {"bar", 17}, {"foos", 19}, {"", 0}});

        EXPECT_TRUE(trie["foo"]);
        EXPECT_TRUE(trie["bar"]);
        EXPECT_TRUE(trie["foos"]);
        EXPECT_TRUE(trie[""]);
        EXPECT_FALSE(trie["other"]);

        // boost::optional/std::optional style
        EXPECT_EQ(*trie["foo"], 13);
        EXPECT_EQ(*trie["bar"], 17);
        EXPECT_EQ(*trie["foos"], 19);
        EXPECT_EQ(*trie[""], 0);

        // new style
        EXPECT_EQ(trie["foo"], 13);
        EXPECT_EQ(trie["bar"], 17);
        EXPECT_EQ(trie["foos"], 19);
        EXPECT_EQ(trie[""], 0);
    }

    {
        boost::text::trie_map<std::string, int> const trie(
            {{"foo", 13}, {"bar", 17}, {"foos", 19}, {"", 0}});

        if (trie["foo"])
            EXPECT_TRUE(true);
        else
            EXPECT_TRUE(false);
        if (trie["bar"])
            EXPECT_TRUE(true);
        else
            EXPECT_TRUE(false);
        if (trie["foos"])
            EXPECT_TRUE(true);
        else
            EXPECT_TRUE(false);
        if (trie[""])
            EXPECT_TRUE(true);
        else
            EXPECT_TRUE(false);
        if (trie["other"])
            EXPECT_TRUE(false);
        else
            EXPECT_TRUE(true);
    }

    {
        boost::text::trie_map<std::string, int> int_counts;
        auto y = int_counts["Hello"];
        (void)y;
        if (int_counts["Hello"])
            EXPECT_TRUE(false);
        else
            EXPECT_TRUE(true);
    }
}

TEST(trie_map, insert)
{
    boost::text::trie_map<std::string, int> trie;

    auto result = trie.insert("", -214);
    EXPECT_EQ(result.iter, trie.find(""));
    EXPECT_TRUE(result.inserted);

    result = trie.insert("", -214);
    EXPECT_EQ(result.iter, trie.find(""));
    EXPECT_FALSE(result.inserted);

    result = trie.insert("", 0);
    EXPECT_EQ(result.iter, trie.find(""));
    EXPECT_FALSE(result.inserted);
}

TEST(trie_map1, erase)
{
    {
        boost::text::trie_map<std::string, int> trie(
            {{"foo", 13}, {"bar", 17}, {"foos", 19}, {"", 42}});

        auto it = trie.find("foo");
        auto next_key = std::next(it)->key;
        it = trie.erase(it);
        EXPECT_EQ(it->key, next_key);

        it = trie.find("bar");
        next_key = std::next(it)->key;
        it = trie.erase(it);
        EXPECT_EQ(it->key, next_key);

        it = trie.find("foos");
        it = trie.erase(it);
        EXPECT_EQ(it, trie.end());

        it = trie.find("");
        it = trie.erase(it);
        EXPECT_EQ(it, trie.end());
    }

    {
        // Sequence generated by the fuzz test.
        boost::text::trie_map<std::string, int> trie;
        trie.insert(" )", 538976288);      // key.size()=2
        trie.insert(" )", 538976288);      // key.size()=2
        trie.insert(" )", 538976288);      // key.size()=2
        trie.insert("  )", 538976288);     // key.size()=3
        trie.insert("  )", 538976288);     // key.size()=3
        trie.insert("  )", 538976288);     // key.size()=3
        trie.insert("  )", 538976288);     // key.size()=3
        trie.insert("' )", 538976288);     // key.size()=3
        trie.insert("' )", 538976288);     // key.size()=3
        trie.insert(" ` `' )", 660611168); // key.size()=7
        trie.insert(" ` `' )", 660611168); // key.size()=7
        trie.insert(" ` `* )", 660611168); // key.size()=7
        trie.insert(" ` `* )", 660611168); // key.size()=7
        trie.insert(" `!`* )", 660611168); // key.size()=7
        trie.insert(" `!`* )", 660611168); // key.size()=7
        trie.insert(" `*`  )", 660611168); // key.size()=7
        trie.insert(" `*`  )", 660611168); // key.size()=7

        auto it = trie.find("  )");
        auto next_key = std::next(it)->key;
        it = trie.erase(it);
        EXPECT_EQ(it->key, next_key);

        trie.erase(trie.begin(), trie.end());
        EXPECT_EQ(trie.size(), 0u);
        EXPECT_TRUE(trie.empty());
    }
}

TEST(trie_node_t, all)
{
    using node_t = text::detail::
        trie_node_t<text::detail::index_within_parent_t, std::string, int>;

    {
        node_t node;
        EXPECT_FALSE(node.value());
        EXPECT_EQ(node.parent(), nullptr);
        EXPECT_TRUE(node.empty());
        EXPECT_EQ(node.size(), 0u);
        EXPECT_EQ(node.begin(), node.end());
        EXPECT_EQ(node.lower_bound('z', text::less{}), node.end());
        EXPECT_EQ(node.find('z', text::less{}), node.end());
        EXPECT_EQ(node.child('z', text::less{}), nullptr);
    }

    {
        node_t const node(nullptr);
        EXPECT_FALSE(node.value());
        EXPECT_EQ(node.parent(), nullptr);
        EXPECT_TRUE(node.empty());
        EXPECT_EQ(node.size(), 0u);
        EXPECT_EQ(node.begin(), node.end());
        EXPECT_EQ(node.lower_bound('z', text::less{}), node.end());
        EXPECT_EQ(node.find('z', text::less{}), node.end());
        EXPECT_EQ(node.child('z', text::less{}), nullptr);
    }

    {
        node_t root;

        std::unique_ptr<node_t> leaf_z(new node_t(&root));
        node_t * const z_ptr = leaf_z.get();
        root.insert('z', text::less{}, std::move(leaf_z));
        std::unique_ptr<node_t> leaf_a(new node_t(&root));
        node_t * const a_ptr = leaf_a.get();
        root.insert('a', text::less{}, std::move(leaf_a));

        EXPECT_FALSE(root.value());
        EXPECT_EQ(root.parent(), nullptr);
        EXPECT_EQ(root.min_child(), a_ptr);
        EXPECT_EQ(root.max_child(), z_ptr);
        EXPECT_FALSE(root.empty());
        EXPECT_EQ(root.size(), 2u);
        EXPECT_FALSE(root.min_value());
        EXPECT_FALSE(root.max_value());
        EXPECT_NE(root.begin(), root.end());
        EXPECT_EQ(root.lower_bound('a', text::less{}), root.begin());
        EXPECT_EQ(root.find('a', text::less{}), root.begin());
        EXPECT_EQ(root.child('a', text::less{}), a_ptr);
        EXPECT_EQ(root.lower_bound('z', text::less{}), ++root.begin());
        EXPECT_EQ(root.find('z', text::less{}), ++root.begin());
        EXPECT_EQ(root.child('z', text::less{}), z_ptr);

        root.erase(std::size_t(0));

        EXPECT_FALSE(root.value());
        EXPECT_EQ(root.parent(), nullptr);
        EXPECT_EQ(root.min_child(), z_ptr);
        EXPECT_EQ(root.max_child(), z_ptr);
        EXPECT_FALSE(root.empty());
        EXPECT_EQ(root.size(), 1u);
        EXPECT_FALSE(root.min_value());
        EXPECT_FALSE(root.max_value());
        EXPECT_NE(root.begin(), root.end());
        EXPECT_EQ(root.lower_bound('a', text::less{}), root.begin());
        EXPECT_EQ(root.find('a', text::less{}), root.end());
        EXPECT_EQ(root.child('a', text::less{}), nullptr);
        EXPECT_EQ(root.lower_bound('z', text::less{}), root.begin());
        EXPECT_EQ(root.find('z', text::less{}), root.begin());
        EXPECT_EQ(root.child('z', text::less{}), z_ptr);
    }

    {
        node_t root_;

        std::unique_ptr<node_t> leaf_z(new node_t(&root_));
        node_t * const z_ptr = leaf_z.get();
        root_.insert('z', text::less{}, std::move(leaf_z));
        std::unique_ptr<node_t> leaf_a(new node_t(&root_));
        node_t * const a_ptr = leaf_a.get();
        root_.insert('a', text::less{}, std::move(leaf_a));

        node_t const & root = root_;

        EXPECT_FALSE(root.value());
        EXPECT_EQ(root.parent(), nullptr);
        EXPECT_EQ(root.min_child(), a_ptr);
        EXPECT_EQ(root.max_child(), z_ptr);
        EXPECT_FALSE(root.empty());
        EXPECT_EQ(root.size(), 2u);
        EXPECT_FALSE(root.min_value());
        EXPECT_FALSE(root.max_value());
        EXPECT_NE(root.begin(), root.end());
        EXPECT_EQ(root.lower_bound('a', text::less{}), root.begin());
        EXPECT_EQ(root.find('a', text::less{}), root.begin());
        EXPECT_EQ(root.child('a', text::less{}), a_ptr);
        EXPECT_EQ(root.lower_bound('z', text::less{}), ++root.begin());
        EXPECT_EQ(root.find('z', text::less{}), ++root.begin());
        EXPECT_EQ(root.child('z', text::less{}), z_ptr);

        root_.erase(std::size_t(0));

        EXPECT_FALSE(root.value());
        EXPECT_EQ(root.parent(), nullptr);
        EXPECT_EQ(root.min_child(), z_ptr);
        EXPECT_EQ(root.max_child(), z_ptr);
        EXPECT_FALSE(root.empty());
        EXPECT_EQ(root.size(), 1u);
        EXPECT_FALSE(root.min_value());
        EXPECT_FALSE(root.max_value());
        EXPECT_NE(root.begin(), root.end());
        EXPECT_EQ(root.lower_bound('a', text::less{}), root.begin());
        EXPECT_EQ(root.find('a', text::less{}), root.end());
        EXPECT_EQ(root.child('a', text::less{}), nullptr);
        EXPECT_EQ(root.lower_bound('z', text::less{}), root.begin());
        EXPECT_EQ(root.find('z', text::less{}), root.begin());
        EXPECT_EQ(root.child('z', text::less{}), z_ptr);
    }
}
