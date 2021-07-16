// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
#define BOOST_TEXT_TESTING
#include <boost/text/unencoded_rope.hpp>

#include <gtest/gtest.h>


using boost::text::unencoded_rope;
using boost::text::string_view;
using namespace boost::text::detail;


inline node_ptr<char, std::string>
make_interior_with_leaves(char const * leaf_name, int leaves)
{
    interior_node_t<char, std::string> * int_node = nullptr;
    node_ptr<char, std::string> node(int_node = new_interior_node<char, std::string>());
    int_node->children_.push_back(make_node<std::string>(leaf_name));
    int_node->keys_.push_back(size(int_node->children_[0].get()));
    for (int i = 1; i < leaves; ++i) {
        int_node->children_.push_back(make_node<std::string>(leaf_name));
        int_node->keys_.push_back(
            int_node->keys_.back() + size(int_node->children_[i].get()));
    }
    return node;
}

template<int SizeLeft, int SizeCenter, int SizeRight>
node_ptr<char, std::string> make_tree_left_center_right()
{
    interior_node_t<char, std::string> * int_root = nullptr;
    node_ptr<char, std::string> root(int_root = new_interior_node<char, std::string>());

    node_ptr<char, std::string> left = make_interior_with_leaves("left", SizeLeft);

    int_root->children_.push_back(left);
    int_root->keys_.push_back(size(left.get()));

    if (SizeCenter != -1) {
        node_ptr<char, std::string> center =
            make_interior_with_leaves("center", SizeCenter);

        int_root->children_.push_back(center);
        int_root->keys_.push_back(int_root->keys_.back() + size(center.get()));
    }

    node_ptr<char, std::string> right = make_interior_with_leaves("right", SizeRight);

    int_root->children_.push_back(right);
    int_root->keys_.push_back(int_root->keys_.back() + size(right.get()));

    return root;
}

template<int SizeLeft, int SizeRight>
node_ptr<char, std::string> make_tree_left_right()
{
    return make_tree_left_center_right<SizeLeft, -1, SizeRight>();
}

inline node_ptr<char, std::string> make_tree_left_max()
{
    return make_tree_left_right<max_children, max_children - 1>();
}

inline node_ptr<char, std::string> make_tree_left_min()
{
    return make_tree_left_right<min_children, max_children - 1>();
}


TEST(rope_btree, test_btree_split_child)
{
    node_ptr<char, std::string> root = make_tree_left_max();
    node_ptr<char, std::string> root_2 = btree_split_child(root, 0);

    EXPECT_EQ(root->refs_, 1);
    EXPECT_EQ(root_2->refs_, 1);

    EXPECT_EQ(children(root_2).size(), 3u);
    EXPECT_EQ(keys(root_2)[0], max_children * 2);
    EXPECT_EQ(keys(root_2)[1], max_children * 2 * 2);
    EXPECT_EQ(keys(root_2)[2], max_children * 2 * 2 + (max_children - 1) * 5);

    EXPECT_EQ(num_children(children(root_2)[0]), min_children);
    EXPECT_EQ(keys(children(root_2)[0]).size(), min_children);
    EXPECT_EQ(keys(children(root_2)[0])[0], 4u);
    EXPECT_EQ(keys(children(root_2)[0])[1], 8u);
    EXPECT_EQ(keys(children(root_2)[0])[2], 12u);
    EXPECT_EQ(keys(children(root_2)[0])[3], 16u);

    EXPECT_EQ(num_children(children(root_2)[1]), min_children);
    EXPECT_EQ(keys(children(root_2)[1]).size(), min_children);
    EXPECT_EQ(keys(children(root_2)[1])[0], 4u);
    EXPECT_EQ(keys(children(root_2)[1])[1], 8u);
    EXPECT_EQ(keys(children(root_2)[1])[2], 12u);
    EXPECT_EQ(keys(children(root_2)[1])[3], 16u);
}

TEST(rope_btree, test_btree_split_child_extra_ref)
{
    node_ptr<char, std::string> root = make_tree_left_max();
    node_ptr<char, std::string> extra_ref = root;
    node_ptr<char, std::string> root_2 = btree_split_child(root, 0);

    EXPECT_EQ(extra_ref->refs_, 2);
    EXPECT_EQ(root_2->refs_, 1);

    EXPECT_EQ(children(root_2).size(), 3u);
    EXPECT_EQ(keys(root_2)[0], max_children * 2);
    EXPECT_EQ(keys(root_2)[1], max_children * 2 * 2);
    EXPECT_EQ(keys(root_2)[2], max_children * 2 * 2 + (max_children - 1) * 5);

    EXPECT_EQ(num_children(children(root_2)[0]), min_children);
    EXPECT_EQ(keys(children(root_2)[0]).size(), min_children);
    EXPECT_EQ(keys(children(root_2)[0])[0], 4u);
    EXPECT_EQ(keys(children(root_2)[0])[1], 8u);
    EXPECT_EQ(keys(children(root_2)[0])[2], 12u);
    EXPECT_EQ(keys(children(root_2)[0])[3], 16u);

    EXPECT_EQ(num_children(children(root_2)[1]), min_children);
    EXPECT_EQ(keys(children(root_2)[1]).size(), min_children);
    EXPECT_EQ(keys(children(root_2)[1])[0], 4u);
    EXPECT_EQ(keys(children(root_2)[1])[1], 8u);
    EXPECT_EQ(keys(children(root_2)[1])[2], 12u);
    EXPECT_EQ(keys(children(root_2)[1])[3], 16u);
}

TEST(rope_btree, test_btree_split_leaf)
{
    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> left = children(root)[0];
        btree_split_leaf(left, 1, 4);
        EXPECT_EQ(size(children(left)[1].get()), 4u);
    }

    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> left = children(root)[0];
        left = btree_split_leaf(left, 1, 5);

        EXPECT_EQ(num_children(left), min_children + 1);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 1u);
        EXPECT_EQ(keys(left)[1], 5u);
        EXPECT_EQ(size(children(left)[2].get()), 3u);
        EXPECT_EQ(keys(left)[2], 8u);
        EXPECT_EQ(keys(left)[3], 12u);
        EXPECT_EQ(keys(left)[4], 16u);
    }

    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> left = children(root)[0];

        // Take an extra reference to the child begin split.
        node_ptr<char, std::string> left_1 = children(left)[1];

        left = btree_split_leaf(left, 1, 5);

        EXPECT_EQ(num_children(left), min_children + 1);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 1u);
        EXPECT_EQ(keys(left)[1], 5u);
        EXPECT_EQ(size(children(left)[2].get()), 3u);
        EXPECT_EQ(keys(left)[2], 8u);
        EXPECT_EQ(keys(left)[3], 12u);
        EXPECT_EQ(keys(left)[4], 16u);

        EXPECT_EQ(size(left_1.get()), 4u);
    }
}

TEST(rope_btree, test_btree_split_leaf_extra_ref)
{
    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> extra_ref = left;
        left = btree_split_leaf(left, 1, 4);
        EXPECT_EQ(left->refs_, 3);
        EXPECT_EQ(extra_ref->refs_, 3);
        EXPECT_EQ(size(children(left)[1].get()), 4u);
    }

    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> extra_ref = left;
        left = btree_split_leaf(left, 1, 5);

        EXPECT_EQ(left->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(num_children(left), min_children + 1);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 1u);
        EXPECT_EQ(keys(left)[1], 5u);
        EXPECT_EQ(size(children(left)[2].get()), 3u);
        EXPECT_EQ(keys(left)[2], 8u);
        EXPECT_EQ(keys(left)[3], 12u);
        EXPECT_EQ(keys(left)[4], 16u);
    }

    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> extra_ref = left;

        // Take an extra reference to the child begin split.
        node_ptr<char, std::string> left_1 = children(left)[1];

        left = btree_split_leaf(left, 1, 5);

        EXPECT_EQ(left->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(num_children(left), min_children + 1);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 1u);
        EXPECT_EQ(keys(left)[1], 5u);
        EXPECT_EQ(size(children(left)[2].get()), 3u);
        EXPECT_EQ(keys(left)[2], 8u);
        EXPECT_EQ(keys(left)[3], 12u);
        EXPECT_EQ(keys(left)[4], 16u);

        EXPECT_EQ(size(left_1.get()), 4u);
    }
}

TEST(rope_btree, test_btree_insert_nonfull)
{
    // Insert into half-full interior child, then between existing leaves.
    {
        node_ptr<char, std::string> root = make_tree_left_min();

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 1);

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 4, make_node<std::string>("new node"));

        left = children(new_root)[0];
        right = children(new_root)[1];

        EXPECT_EQ(num_children(new_root), 2u);
        EXPECT_EQ(num_children(left), min_children + 1);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 8u);
        EXPECT_EQ(keys(left)[1], 12u);
        EXPECT_EQ(size(children(left)[2].get()), 4u);
        EXPECT_EQ(keys(left)[2], 16u);
        EXPECT_EQ(size(children(left)[3].get()), 4u);
        EXPECT_EQ(keys(left)[3], 20u);
        EXPECT_EQ(keys(left)[4], 24u);
    }

    // Insert into half-full interior child, then into the middle of an
    // existing leaf.
    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> new_node = make_node<std::string>("new node");

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 1);

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 5, make_node<std::string>("new node"));

        left = children(new_root)[0];
        right = children(new_root)[1];

        EXPECT_EQ(num_children(new_root), 2u);
        EXPECT_EQ(num_children(left), min_children + 2);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 1u);
        EXPECT_EQ(keys(left)[1], 5u);
        EXPECT_EQ(size(children(left)[2].get()), 8u);
        EXPECT_EQ(keys(left)[2], 13u);
        EXPECT_EQ(size(children(left)[3].get()), 3u);
        EXPECT_EQ(keys(left)[3], 16u);
        EXPECT_EQ(keys(left)[4], 20u);
        EXPECT_EQ(keys(left)[5], 24u);
    }

    // Insert into full interior child, then between existing leaves.
    {
        node_ptr<char, std::string> root = make_tree_left_max();

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), max_children);
        EXPECT_EQ(num_children(right), max_children - 1);

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 4, make_node<std::string>("new node"));

        left = children(new_root)[0];
        right = children(new_root)[1];

        EXPECT_EQ(num_children(new_root), 3u);
        EXPECT_EQ(num_children(left), min_children + 1);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 8u);
        EXPECT_EQ(keys(left)[1], 12u);
        EXPECT_EQ(size(children(left)[2].get()), 4u);
        EXPECT_EQ(keys(left)[2], 16u);
        EXPECT_EQ(size(children(left)[3].get()), 4u);
        EXPECT_EQ(keys(left)[3], 20u);
        EXPECT_EQ(keys(left)[4], 24u);
    }

    // Insert into almost-full interior child, then between existing leaves.
    {
        node_ptr<char, std::string> root = make_tree_left_max();

        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(right), max_children - 1);

        node_ptr<char, std::string> new_root = btree_insert_nonfull(
            root, size(root.get()) - 5, make_node<std::string>("new node"));

        EXPECT_EQ(num_children(new_root), 3u);

        node_ptr<char, std::string> new_right = children(new_root)[2];
        EXPECT_EQ(num_children(new_right), min_children);

        EXPECT_EQ(size(children(new_right)[min_children - 2].get()), 8u);
        EXPECT_EQ(
            keys(new_right)[min_children - 2], (min_children - 2) * 5 + 8);
        EXPECT_EQ(size(children(new_right)[min_children - 1].get()), 5u);
        EXPECT_EQ(
            keys(new_right)[min_children - 1], (min_children - 2) * 5 + 8 + 5);
    }

    // Insert into almost-full interior child, then into the middle of an
    // existing leaf.
    {
        node_ptr<char, std::string> root = make_tree_left_max();

        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(right), max_children - 1);

        node_ptr<char, std::string> new_root = btree_insert_nonfull(
            root, size(root.get()) - 2, make_node<std::string>("new node"));

        EXPECT_EQ(num_children(new_root), 3u);

        node_ptr<char, std::string> new_right = children(new_root)[2];
        EXPECT_EQ(num_children(new_right), min_children + 1);

        EXPECT_EQ(size(children(new_right)[min_children - 2].get()), 3u);
        EXPECT_EQ(
            keys(new_right)[min_children - 2], (min_children - 2) * 5 + 3);
        EXPECT_EQ(size(children(new_right)[min_children - 1].get()), 8u);
        EXPECT_EQ(
            keys(new_right)[min_children - 1], (min_children - 2) * 5 + 3 + 8);
        EXPECT_EQ(size(children(new_right)[min_children].get()), 2u);
        EXPECT_EQ(
            keys(new_right)[min_children], (min_children - 2) * 5 + 3 + 8 + 2);
    }


    // Insert into almost-full interior child, then after the last leaf.
    {
        node_ptr<char, std::string> root = make_tree_left_max();

        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(right), max_children - 1);

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, size(root.get()), make_node<std::string>("new node"));

        EXPECT_EQ(num_children(new_root), 3u);

        node_ptr<char, std::string> new_right = children(new_root)[2];
        EXPECT_EQ(num_children(new_right), min_children);

        EXPECT_EQ(size(children(new_right)[min_children - 1].get()), 8u);
        EXPECT_EQ(
            keys(new_right)[min_children - 1], (min_children - 1) * 5 + 8);
    }

    // Copy vs. mutation coverage.

    // No nodes copied.
    {
        node_ptr<char, std::string> root = make_tree_left_min();

        node_ptr<char, std::string> left = children(root)[0];

        EXPECT_EQ(num_children(left), min_children);

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 4, make_node<std::string>("new node"));

        left = children(new_root)[0];

        EXPECT_EQ(num_children(new_root), 2u);
        EXPECT_EQ(num_children(left), min_children + 1u);
        EXPECT_NE(root.as_interior(), new_root.as_interior());
    }

    // Root copied.
    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> root_2 = root;

        node_ptr<char, std::string> left = children(root)[0];

        EXPECT_EQ(num_children(left), min_children);

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 4, make_node<std::string>("new node"));

        left = children(new_root)[0];

        EXPECT_EQ(num_children(new_root), 2u);
        EXPECT_EQ(num_children(left), min_children + 1u);
        EXPECT_NE(root.as_interior(), new_root.as_interior());
        EXPECT_EQ(root.as_interior(), root_2.as_interior());
    }

    // Interior node copied.
    {
        node_ptr<char, std::string> root = make_tree_left_min();

        node_ptr<char, std::string> left = children(root)[0];

        EXPECT_EQ(num_children(left), min_children);

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 4, make_node<std::string>("new node"));

        node_ptr<char, std::string> new_left = children(new_root)[0];

        EXPECT_EQ(num_children(new_root), 2u);
        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(new_left), min_children + 1);
    }
}

TEST(rope_btree, test_btree_insert_nonfull_extra_ref)
{
    // Insert into half-full interior child, then between existing leaves.
    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> extra_ref = root;

        {
            node_ptr<char, std::string> left = children(root)[0];
            node_ptr<char, std::string> right = children(root)[1];

            EXPECT_EQ(num_children(left), min_children);
            EXPECT_EQ(num_children(right), max_children - 1);
        }

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 4, make_node<std::string>("new node"));

        EXPECT_EQ(new_root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        node_ptr<char, std::string> left = children(new_root)[0];

        EXPECT_EQ(num_children(new_root), 2u);
        EXPECT_EQ(num_children(left), min_children + 1);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 8u);
        EXPECT_EQ(keys(left)[1], 12u);
        EXPECT_EQ(size(children(left)[2].get()), 4u);
        EXPECT_EQ(keys(left)[2], 16u);
        EXPECT_EQ(size(children(left)[3].get()), 4u);
        EXPECT_EQ(keys(left)[3], 20u);
        EXPECT_EQ(keys(left)[4], 24u);
    }

    // Insert into half-full interior child, then into the middle of an
    // existing leaf.
    {
        node_ptr<char, std::string> root = make_tree_left_min();
        node_ptr<char, std::string> new_node = make_node<std::string>("new node");
        node_ptr<char, std::string> extra_ref = root;

        {
            node_ptr<char, std::string> left = children(root)[0];
            node_ptr<char, std::string> right = children(root)[1];

            EXPECT_EQ(num_children(left), min_children);
            EXPECT_EQ(num_children(right), max_children - 1);
        }

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 5, make_node<std::string>("new node"));

        EXPECT_EQ(new_root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        node_ptr<char, std::string> left = children(new_root)[0];

        EXPECT_EQ(num_children(new_root), 2u);
        EXPECT_EQ(num_children(left), min_children + 2);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 1u);
        EXPECT_EQ(keys(left)[1], 5u);
        EXPECT_EQ(size(children(left)[2].get()), 8u);
        EXPECT_EQ(keys(left)[2], 13u);
        EXPECT_EQ(size(children(left)[3].get()), 3u);
        EXPECT_EQ(keys(left)[3], 16u);
        EXPECT_EQ(keys(left)[4], 20u);
        EXPECT_EQ(keys(left)[5], 24u);
    }

    // Insert into full interior child, then between existing leaves.
    {
        node_ptr<char, std::string> root = make_tree_left_max();
        node_ptr<char, std::string> extra_ref = root;

        {
            node_ptr<char, std::string> left = children(root)[0];
            node_ptr<char, std::string> right = children(root)[1];

            EXPECT_EQ(num_children(left), max_children);
            EXPECT_EQ(num_children(right), max_children - 1);
        }

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, 4, make_node<std::string>("new node"));

        EXPECT_EQ(new_root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        node_ptr<char, std::string> left = children(new_root)[0];

        EXPECT_EQ(num_children(new_root), 3u);
        EXPECT_EQ(num_children(left), min_children + 1);

        EXPECT_EQ(keys(left)[0], 4u);
        EXPECT_EQ(size(children(left)[1].get()), 8u);
        EXPECT_EQ(keys(left)[1], 12u);
        EXPECT_EQ(size(children(left)[2].get()), 4u);
        EXPECT_EQ(keys(left)[2], 16u);
        EXPECT_EQ(size(children(left)[3].get()), 4u);
        EXPECT_EQ(keys(left)[3], 20u);
        EXPECT_EQ(keys(left)[4], 24u);
    }

    // Insert into almost-full interior child, then between existing leaves.
    {
        node_ptr<char, std::string> root = make_tree_left_max();
        node_ptr<char, std::string> extra_ref = root;

        {
            node_ptr<char, std::string> right = children(root)[1];

            EXPECT_EQ(num_children(right), max_children - 1);
        }

        node_ptr<char, std::string> new_root = btree_insert_nonfull(
            root, size(root.get()) - 5, make_node<std::string>("new node"));

        EXPECT_EQ(new_root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(num_children(new_root), 3u);

        node_ptr<char, std::string> new_right = children(new_root)[2];
        EXPECT_EQ(num_children(new_right), min_children);

        EXPECT_EQ(size(children(new_right)[min_children - 2].get()), 8u);
        EXPECT_EQ(
            keys(new_right)[min_children - 2], (min_children - 2) * 5 + 8);
        EXPECT_EQ(size(children(new_right)[min_children - 1].get()), 5u);
        EXPECT_EQ(
            keys(new_right)[min_children - 1], (min_children - 2) * 5 + 8 + 5);
    }

    // Insert into almost-full interior child, then into the middle of an
    // existing leaf.
    {
        node_ptr<char, std::string> root = make_tree_left_max();
        node_ptr<char, std::string> extra_ref = root;

        {
            node_ptr<char, std::string> right = children(root)[1];

            EXPECT_EQ(num_children(right), max_children - 1);
        }

        node_ptr<char, std::string> new_root = btree_insert_nonfull(
            root, size(root.get()) - 2, make_node<std::string>("new node"));

        EXPECT_EQ(new_root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(num_children(new_root), 3u);

        node_ptr<char, std::string> new_right = children(new_root)[2];
        EXPECT_EQ(num_children(new_right), min_children + 1);

        EXPECT_EQ(size(children(new_right)[min_children - 2].get()), 3u);
        EXPECT_EQ(
            keys(new_right)[min_children - 2], (min_children - 2) * 5 + 3);
        EXPECT_EQ(size(children(new_right)[min_children - 1].get()), 8u);
        EXPECT_EQ(
            keys(new_right)[min_children - 1], (min_children - 2) * 5 + 3 + 8);
        EXPECT_EQ(size(children(new_right)[min_children].get()), 2u);
        EXPECT_EQ(
            keys(new_right)[min_children], (min_children - 2) * 5 + 3 + 8 + 2);
    }


    // Insert into almost-full interior child, then after the last leaf.
    {
        node_ptr<char, std::string> root = make_tree_left_max();
        node_ptr<char, std::string> extra_ref = root;

        {
            node_ptr<char, std::string> right = children(root)[1];

            EXPECT_EQ(num_children(right), max_children - 1);
        }

        node_ptr<char, std::string> new_root =
            btree_insert_nonfull(root, size(root.get()), make_node<std::string>("new node"));

        EXPECT_EQ(new_root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(num_children(new_root), 3u);

        node_ptr<char, std::string> new_right = children(new_root)[2];
        EXPECT_EQ(num_children(new_right), min_children);

        EXPECT_EQ(size(children(new_right)[min_children - 1].get()), 8u);
        EXPECT_EQ(
            keys(new_right)[min_children - 1], (min_children - 1) * 5 + 8);
    }
}

int height_at(node_ptr<char, std::string> const & node, std::size_t at)
{
    found_leaf<char, std::string> found;
    find_leaf(node, at, found);
    return (int)found.path_.size();
}

void check_leaf_heights(node_ptr<char, std::string> const & node)
{
    found_leaf<char, std::string> found;
    find_leaf(node, 0, found);
    int const first_leaf_height = (int)found.path_.size();
    std::size_t offset = 0;
    while (offset != size(node.get())) {
        EXPECT_EQ(height_at(node, offset), first_leaf_height);
        found_leaf<char, std::string> found_next;
        find_leaf(node, offset, found_next);
        offset += found_next.leaf_->as_leaf()->size();
    }
}

TEST(rope_btree, test_btree_insert)
{
    {
        node_ptr<char, std::string> root = make_node<std::string>("root");
        root = btree_insert(root, 0, make_node<std::string>("new"));

        EXPECT_FALSE(root->leaf_);
        EXPECT_EQ(num_children(root), 2u);

        check_leaf_heights(root);
    }

    {
        node_ptr<char, std::string> root = make_node<std::string>("root");
        root = btree_insert(root, 4, make_node<std::string>("new"));

        EXPECT_FALSE(root->leaf_);
        EXPECT_EQ(num_children(root), 2u);

        check_leaf_heights(root);
    }

    {
        node_ptr<char, std::string> root = make_node<std::string>("root");
        root = btree_insert(root, 2, make_node<std::string>("new"));

        EXPECT_FALSE(root->leaf_);
        EXPECT_EQ(num_children(root), 3u);

        check_leaf_heights(root);
    }

    {
        node_ptr<char, std::string> root =
            make_interior_with_leaves("child", max_children - 1);
        root = btree_insert(root, 2, make_node<std::string>("new 1"));

        EXPECT_EQ(num_children(root), 2u);

        check_leaf_heights(root);
    }

    {
        node_ptr<char, std::string> root =
            make_interior_with_leaves("child", max_children);
        root = btree_insert(root, 2, make_node<std::string>("new 1"));

        EXPECT_EQ(num_children(root), 2u);

        check_leaf_heights(root);
    }

    // Check that many inserts maintains balance.
    {
        node_ptr<char, std::string> root = make_node<std::string>("node");

        int const N = 100000;
        for (int i = 0; i < N; ++i) {
            root = btree_insert(root, 2, make_node<std::string>("new node"));
        }

        check_leaf_heights(root);

        std::cout << "N=" << N << " leaves gives a tree of height "
                  << height_at(root, 0) << "\n";
    }
}

TEST(rope_btree, test_btree_insert_extra_ref)
{
    {
        node_ptr<char, std::string> root = make_node<std::string>("root");
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;
        root = btree_insert(root, 0, make_node<std::string>("new"));

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 3);

        EXPECT_FALSE(root->leaf_);
        EXPECT_EQ(num_children(root), 2u);

        check_leaf_heights(root);
    }

    {
        node_ptr<char, std::string> root = make_node<std::string>("root");
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;
        root = btree_insert(root, 4, make_node<std::string>("new"));

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 3);

        EXPECT_FALSE(root->leaf_);
        EXPECT_EQ(num_children(root), 2u);

        check_leaf_heights(root);
    }

    {
        node_ptr<char, std::string> root = make_node<std::string>("root");
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;
        root = btree_insert(root, 2, make_node<std::string>("new"));

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 4);

        EXPECT_FALSE(root->leaf_);
        EXPECT_EQ(num_children(root), 3u);

        check_leaf_heights(root);
    }

    {
        node_ptr<char, std::string> root =
            make_interior_with_leaves("child", max_children - 1);
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;
        root = btree_insert(root, 2, make_node<std::string>("new 1"));

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(num_children(root), 2u);

        check_leaf_heights(root);
    }

    {
        node_ptr<char, std::string> root =
            make_interior_with_leaves("child", max_children);
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;
        root = btree_insert(root, 2, make_node<std::string>("new 1"));

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(num_children(root), 2u);

        check_leaf_heights(root);
    }

    // Check that many inserts maintains balance.
    {
        node_ptr<char, std::string> root = make_node<std::string>("node");
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;

        int const N = 100000;
        for (int i = 0; i < N; ++i) {
            root = btree_insert(root, 2, make_node<std::string>("new node"));
        }

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 4);

        check_leaf_heights(root);

        std::cout << "N=" << N << " leaves gives a tree of height "
                  << height_at(root, 0) << "\n";
    }
}

TEST(rope_btree, test_btree_erase_entire_node_leaf_children)
{
    {
        node_ptr<char, std::string> root = make_interior_with_leaves("leaf", 3);

        EXPECT_EQ(num_children(root), 3u);

        root = btree_erase(root, 0, children(root)[0].as_leaf());

        EXPECT_EQ(keys(root)[0], 4u);
        EXPECT_EQ(size(children(root)[0].get()), 4u);
        EXPECT_EQ(keys(root)[1], 8u);
        EXPECT_EQ(size(children(root)[1].get()), 4u);
    }

    {
        node_ptr<char, std::string> root = make_interior_with_leaves("leaf", 3);

        EXPECT_EQ(num_children(root), 3u);

        root = btree_erase(root, 4, children(root)[1].as_leaf());

        EXPECT_EQ(keys(root)[0], 4u);
        EXPECT_EQ(size(children(root)[0].get()), 4u);
        EXPECT_EQ(keys(root)[1], 8u);
        EXPECT_EQ(size(children(root)[1].get()), 4u);
    }

    {
        node_ptr<char, std::string> root = make_interior_with_leaves("leaf", 3);

        EXPECT_EQ(num_children(root), 3u);

        root = btree_erase(root, 8, children(root)[2].as_leaf());

        EXPECT_EQ(keys(root)[0], 4u);
        EXPECT_EQ(size(children(root)[0].get()), 4u);
        EXPECT_EQ(keys(root)[1], 8u);
        EXPECT_EQ(size(children(root)[1].get()), 4u);
    }

    {
        node_ptr<char, std::string> root = make_interior_with_leaves("leaf", 3);

        EXPECT_EQ(num_children(root), 3u);

        root = btree_erase(root, 12, children(root)[2].as_leaf());

        EXPECT_EQ(keys(root)[0], 4u);
        EXPECT_EQ(size(children(root)[0].get()), 4u);
        EXPECT_EQ(keys(root)[1], 8u);
        EXPECT_EQ(size(children(root)[1].get()), 4u);
    }


    {
        node_ptr<char, std::string> root;
        {
            interior_node_t<char, std::string> * int_root = nullptr;
            root = node_ptr<char, std::string>(int_root = new_interior_node<char, std::string>());
            int_root->children_.push_back(make_node<std::string>("left"));
            int_root->keys_.push_back(size(int_root->children_[0].get()));
            int_root->children_.push_back(make_node<std::string>("right"));
            int_root->keys_.push_back(
                int_root->keys_.back() + size(int_root->children_[1].get()));
        }

        EXPECT_EQ(num_children(root), 2u);

        root = btree_erase(root, 0, children(root)[0].as_leaf());

        EXPECT_TRUE(root->leaf_);
        EXPECT_EQ(size(root.get()), 5u);
    }

    {
        node_ptr<char, std::string> root;
        {
            interior_node_t<char, std::string> * int_root = nullptr;
            root = node_ptr<char, std::string>(int_root = new_interior_node<char, std::string>());
            int_root->children_.push_back(make_node<std::string>("left"));
            int_root->keys_.push_back(size(int_root->children_[0].get()));
            int_root->children_.push_back(make_node<std::string>("right"));
            int_root->keys_.push_back(
                int_root->keys_.back() + size(int_root->children_[1].get()));
        }

        EXPECT_EQ(num_children(root), 2u);

        root = btree_erase(root, 4, children(root)[1].as_leaf());

        EXPECT_TRUE(root->leaf_);
        EXPECT_EQ(size(root.get()), 4u);
    }

    {
        node_ptr<char, std::string> root;
        {
            interior_node_t<char, std::string> * int_root = nullptr;
            root = node_ptr<char, std::string>(int_root = new_interior_node<char, std::string>());
            int_root->children_.push_back(make_node<std::string>("left"));
            int_root->keys_.push_back(size(int_root->children_[0].get()));
            int_root->children_.push_back(make_node<std::string>("right"));
            int_root->keys_.push_back(
                int_root->keys_.back() + size(int_root->children_[1].get()));
        }

        EXPECT_EQ(num_children(root), 2u);

        root = btree_erase(root, 9, children(root)[1].as_leaf());

        EXPECT_TRUE(root->leaf_);
        EXPECT_EQ(size(root.get()), 4u);
    }
}

TEST(rope_btree, test_btree_erase_entire_node_leaf_children_extra_ref)
{
    {
        node_ptr<char, std::string> root = make_interior_with_leaves("leaf", 3);
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;

        EXPECT_EQ(num_children(root), 3u);

        root = btree_erase(root, 0, children(root)[0].as_leaf());

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(keys(root)[0], 4u);
        EXPECT_EQ(size(children(root)[0].get()), 4u);
        EXPECT_EQ(keys(root)[1], 8u);
        EXPECT_EQ(size(children(root)[1].get()), 4u);
    }

    {
        node_ptr<char, std::string> root = make_interior_with_leaves("leaf", 3);
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;

        EXPECT_EQ(num_children(root), 3u);

        root = btree_erase(root, 4, children(root)[1].as_leaf());

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(keys(root)[0], 4u);
        EXPECT_EQ(size(children(root)[0].get()), 4u);
        EXPECT_EQ(keys(root)[1], 8u);
        EXPECT_EQ(size(children(root)[1].get()), 4u);
    }

    {
        node_ptr<char, std::string> root = make_interior_with_leaves("leaf", 3);
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;

        EXPECT_EQ(num_children(root), 3u);

        root = btree_erase(root, 8, children(root)[2].as_leaf());

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(keys(root)[0], 4u);
        EXPECT_EQ(size(children(root)[0].get()), 4u);
        EXPECT_EQ(keys(root)[1], 8u);
        EXPECT_EQ(size(children(root)[1].get()), 4u);
    }

    {
        node_ptr<char, std::string> root = make_interior_with_leaves("leaf", 3);
        node_ptr<char, std::string> extra_ref = root;
        node_ptr<char, std::string> extra_ref_2 = root;

        EXPECT_EQ(num_children(root), 3u);

        root = btree_erase(root, 12, children(root)[2].as_leaf());

        EXPECT_EQ(root->refs_, 1);
        EXPECT_EQ(extra_ref->refs_, 2);

        EXPECT_EQ(keys(root)[0], 4u);
        EXPECT_EQ(size(children(root)[0].get()), 4u);
        EXPECT_EQ(keys(root)[1], 8u);
        EXPECT_EQ(size(children(root)[1].get()), 4u);
    }


    {
        node_ptr<char, std::string> root;
        {
            interior_node_t<char, std::string> * int_root = nullptr;
            root = node_ptr<char, std::string>(int_root = new_interior_node<char, std::string>());
            int_root->children_.push_back(make_node<std::string>("left"));
            int_root->keys_.push_back(size(int_root->children_[0].get()));
            int_root->children_.push_back(make_node<std::string>("right"));
            int_root->keys_.push_back(
                int_root->keys_.back() + size(int_root->children_[1].get()));
        }
        node_ptr<char, std::string> extra_ref = root;

        EXPECT_EQ(num_children(root), 2u);

        root = btree_erase(root, 0, children(root)[0].as_leaf());

        EXPECT_NE(root.get(), extra_ref.get());

        EXPECT_TRUE(root->leaf_);
        EXPECT_EQ(size(root.get()), 5u);
    }

    {
        node_ptr<char, std::string> root;
        {
            interior_node_t<char, std::string> * int_root = nullptr;
            root = node_ptr<char, std::string>(int_root = new_interior_node<char, std::string>());
            int_root->children_.push_back(make_node<std::string>("left"));
            int_root->keys_.push_back(size(int_root->children_[0].get()));
            int_root->children_.push_back(make_node<std::string>("right"));
            int_root->keys_.push_back(
                int_root->keys_.back() + size(int_root->children_[1].get()));
        }
        node_ptr<char, std::string> extra_ref = root;

        EXPECT_EQ(num_children(root), 2u);

        root = btree_erase(root, 4, children(root)[1].as_leaf());

        EXPECT_NE(root.get(), extra_ref.get());

        EXPECT_TRUE(root->leaf_);
        EXPECT_EQ(size(root.get()), 4u);
    }

    {
        node_ptr<char, std::string> root;
        {
            interior_node_t<char, std::string> * int_root = nullptr;
            root = node_ptr<char, std::string>(int_root = new_interior_node<char, std::string>());
            int_root->children_.push_back(make_node<std::string>("left"));
            int_root->keys_.push_back(size(int_root->children_[0].get()));
            int_root->children_.push_back(make_node<std::string>("right"));
            int_root->keys_.push_back(
                int_root->keys_.back() + size(int_root->children_[1].get()));
        }
        node_ptr<char, std::string> extra_ref = root;

        EXPECT_EQ(num_children(root), 2u);

        root = btree_erase(root, 9, children(root)[1].as_leaf());

        EXPECT_NE(root.get(), extra_ref.get());

        EXPECT_TRUE(root->leaf_);
        EXPECT_EQ(size(root.get()), 4u);
    }
}

// The rest of these don't have _extra_ref variants, because the
// rope::operator() tests exercise the shared node cases well enough.

TEST(rope_btree, test_btree_erase_entire_node_interior_children)
{
    // Last interior node has more than min children.
    {
        node_ptr<char, std::string> root = make_tree_left_min();

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 1);

        root =
            btree_erase(root, min_children * 4, children(right)[0].as_leaf());

        left = children(root)[0];
        right = children(root)[1];

        EXPECT_EQ(num_children(root), 2u);
        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 2);

        EXPECT_EQ(keys(left).back(), min_children * 4);
        EXPECT_EQ(keys(right).back(), (max_children - 2) * 5);
    }

    {
        node_ptr<char, std::string> root = make_tree_left_min();

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 1);

        root = btree_erase(
            root,
            min_children * 4 + (max_children - 1) * 5,
            children(right).back().as_leaf());

        left = children(root)[0];
        right = children(root)[1];

        EXPECT_EQ(num_children(root), 2u);
        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 2);

        EXPECT_EQ(keys(left).back(), min_children * 4);
        EXPECT_EQ(keys(right).back(), (max_children - 2) * 5);
    }


    // Last interior node min children, left has min children.
    {
        node_ptr<char, std::string> root =
            make_tree_left_right<min_children, max_children>();

        auto const root_initial_size = size(root.get());

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children);

        root = btree_erase(root, 0, children(left).front().as_leaf());

        left = children(root)[0];
        right = children(root)[1];

        EXPECT_EQ(num_children(root), 2u);
        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 1);

        EXPECT_EQ(keys(root)[0], (min_children - 1) * 4 + 5);
        EXPECT_EQ(keys(root)[1], root_initial_size - 4);
        EXPECT_EQ(keys(left).back(), (min_children - 1) * 4 + 5);
        EXPECT_EQ(keys(right).back(), (max_children - 1) * 5);
    }

    {
        node_ptr<char, std::string> root =
            make_tree_left_right<min_children, max_children>();

        auto const root_initial_size = size(root.get());

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children);

        root = btree_erase(
            root, min_children * 4 - 1, children(left).back().as_leaf());

        left = children(root)[0];
        right = children(root)[1];

        EXPECT_EQ(num_children(root), 2u);
        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 1);

        EXPECT_EQ(keys(root)[0], (min_children - 1) * 4 + 5);
        EXPECT_EQ(keys(root)[1], root_initial_size - 4);
        EXPECT_EQ(keys(left).back(), (min_children - 1) * 4 + 5);
        EXPECT_EQ(keys(right).back(), (max_children - 1) * 5);
    }


    // Last interior node min children, right has min children.
    {
        node_ptr<char, std::string> root =
            make_tree_left_right<max_children, min_children>();

        auto const root_initial_size = size(root.get());

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), max_children);
        EXPECT_EQ(num_children(right), min_children);

        root = btree_erase(
            root, size(root.get()), children(right).back().as_leaf());

        left = children(root)[0];
        right = children(root)[1];

        EXPECT_EQ(num_children(root), 2u);
        EXPECT_EQ(num_children(left), max_children - 1);
        EXPECT_EQ(num_children(right), min_children);

        EXPECT_EQ(keys(root)[0], (max_children - 1) * 4);
        EXPECT_EQ(keys(root)[1], root_initial_size - 5);
        EXPECT_EQ(keys(left).back(), (max_children - 1) * 4);
        EXPECT_EQ(keys(right).back(), 4 + (min_children - 1) * 5);
    }

    {
        node_ptr<char, std::string> root =
            make_tree_left_right<max_children, min_children>();

        auto const root_initial_size = size(root.get());

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), max_children);
        EXPECT_EQ(num_children(right), min_children);

        root = btree_erase(
            root, max_children * 4, children(right).front().as_leaf());

        left = children(root)[0];
        right = children(root)[1];

        EXPECT_EQ(num_children(root), 2u);
        EXPECT_EQ(num_children(left), max_children - 1);
        EXPECT_EQ(num_children(right), min_children);

        EXPECT_EQ(keys(root)[0], (max_children - 1) * 4);
        EXPECT_EQ(keys(root)[1], root_initial_size - 5);
        EXPECT_EQ(keys(left).back(), (max_children - 1) * 4);
        EXPECT_EQ(keys(right).back(), 4 + (min_children - 1) * 5);
    }


    // Last interior node min children, both sides have min children.
    {
        node_ptr<char, std::string> root =
            make_tree_left_right<min_children, min_children>();

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), min_children);

        root = btree_erase(
            root, size(root.get()), children(right).back().as_leaf());

        EXPECT_EQ(num_children(root), max_children - 1);

        std::size_t i = 0;
        std::size_t sz = 0;
        for (; i < min_children - 1; ++i) {
            sz += size(children(root)[i].get());
            EXPECT_EQ(keys(root)[i], sz) << "i=" << i;
        }
        for (; i < max_children - 1; ++i) {
            sz += size(children(root)[i].get());
            EXPECT_EQ(keys(root)[i], sz) << "i=" << i;
        }
    }

    {
        node_ptr<char, std::string> root =
            make_tree_left_right<min_children, min_children>();

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), min_children);

        root = btree_erase(root, 0, children(left).front().as_leaf());

        EXPECT_EQ(num_children(root), max_children - 1);

        std::size_t i = 0;
        std::size_t sz = 0;
        for (; i < min_children; ++i) {
            sz += size(children(root)[i].get());
            EXPECT_EQ(keys(root)[i], sz) << "i=" << i;
        }
        for (; i < max_children - 1; ++i) {
            sz += size(children(root)[i].get());
            EXPECT_EQ(keys(root)[i], sz) << "i=" << i;
        }
    }


    // Last interior node min children, all three children have min children.
    {
        node_ptr<char, std::string> root = make_tree_left_center_right<
            min_children,
            min_children,
            min_children>();

        auto const root_initial_size = size(root.get());

        {
            node_ptr<char, std::string> left = children(root)[0];
            node_ptr<char, std::string> center = children(root)[1];
            node_ptr<char, std::string> right = children(root)[2];

            EXPECT_EQ(num_children(left), min_children);
            EXPECT_EQ(num_children(center), min_children);
            EXPECT_EQ(num_children(right), min_children);

            root = btree_erase(root, 0, children(left).front().as_leaf());
        }

        EXPECT_EQ(num_children(root), 2u);

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), max_children - 1);
        EXPECT_EQ(num_children(right), min_children);
        EXPECT_EQ(keys(root)[0], (min_children - 1) * 4 + min_children * 6);
        EXPECT_EQ(keys(root)[1], root_initial_size - 4);
        EXPECT_EQ(keys(left).back(), (min_children - 1) * 4 + min_children * 6);
        EXPECT_EQ(keys(right).back(), min_children * 5);
    }

    {
        node_ptr<char, std::string> root = make_tree_left_center_right<
            min_children,
            min_children,
            min_children>();

        auto const root_initial_size = size(root.get());

        {
            node_ptr<char, std::string> left = children(root)[0];
            node_ptr<char, std::string> center = children(root)[1];
            node_ptr<char, std::string> right = children(root)[2];

            EXPECT_EQ(num_children(left), min_children);
            EXPECT_EQ(num_children(center), min_children);
            EXPECT_EQ(num_children(right), min_children);

            root = btree_erase(
                root, min_children * 4, children(center).front().as_leaf());
        }

        EXPECT_EQ(num_children(root), 2u);

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), max_children - 1);
        EXPECT_EQ(num_children(right), min_children);
        EXPECT_EQ(keys(root)[0], min_children * 4 + (min_children - 1) * 6);
        EXPECT_EQ(keys(root)[1], root_initial_size - 6);
        EXPECT_EQ(keys(left).back(), min_children * 4 + (min_children - 1) * 6);
        EXPECT_EQ(keys(right).back(), min_children * 5);
    }

    {
        node_ptr<char, std::string> root = make_tree_left_center_right<
            min_children,
            min_children,
            min_children>();

        auto const root_initial_size = size(root.get());

        {
            node_ptr<char, std::string> left = children(root)[0];
            node_ptr<char, std::string> center = children(root)[1];
            node_ptr<char, std::string> right = children(root)[2];

            EXPECT_EQ(num_children(left), min_children);
            EXPECT_EQ(num_children(center), min_children);
            EXPECT_EQ(num_children(right), min_children);

            root = btree_erase(
                root, size(root.get()), children(right).back().as_leaf());
        }

        EXPECT_EQ(num_children(root), 2u);

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), min_children);
        EXPECT_EQ(num_children(right), max_children - 1);
        EXPECT_EQ(keys(root)[0], min_children * 4);
        EXPECT_EQ(keys(root)[1], root_initial_size - 5);
        EXPECT_EQ(keys(left).back(), min_children * 4);
        EXPECT_EQ(
            keys(right).back(), min_children * 6 + (min_children - 1) * 5);
    }
}

TEST(rope_btree, test_btree_erase)
{
    // Erasure from a leaf node
    {
        node_ptr<char, std::string> root = make_node<std::string>("sliceable");

        root = btree_erase(root, 0, 9);

        EXPECT_EQ(root.get(), nullptr);
    }

    {
        node_ptr<char, std::string> root = make_node<std::string>("sliceable");

        root = btree_erase(root, 0, 8);

        EXPECT_TRUE(root->leaf_);
        EXPECT_EQ(size(root.get()), 1u);
    }

    {
        node_ptr<char, std::string> root = make_node<std::string>("sliceable");

        root = btree_erase(root, 1, 8);

        EXPECT_FALSE(root->leaf_);
        EXPECT_EQ(size(root.get()), 2u);
    }

    // Erasure from non-leaf nodes, entire segments only

    {
        node_ptr<char, std::string> root =
            make_tree_left_right<max_children, max_children>();

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), max_children);
        EXPECT_EQ(num_children(right), max_children);

        root = btree_erase(root, (max_children - 1) * 4, max_children * 4);

        left = children(root)[0];
        right = children(root)[1];

        EXPECT_EQ(num_children(root), 2u);
        EXPECT_EQ(num_children(left), max_children - 1);
        EXPECT_EQ(num_children(right), max_children);

        EXPECT_EQ(keys(root)[0], (max_children - 1) * 4);
        EXPECT_EQ(keys(root)[1], (max_children - 1) * 4 + max_children * 5);
        EXPECT_EQ(keys(left).back(), (max_children - 1) * 4);
        EXPECT_EQ(keys(right).back(), max_children * 5);
    }

    {
        node_ptr<char, std::string> root =
            make_tree_left_right<max_children, max_children>();

        node_ptr<char, std::string> left = children(root)[0];
        node_ptr<char, std::string> right = children(root)[1];

        EXPECT_EQ(num_children(left), max_children);
        EXPECT_EQ(num_children(right), max_children);

        root = btree_erase(root, max_children * 4, max_children * 4 + 5);

        left = children(root)[0];
        right = children(root)[1];

        EXPECT_EQ(num_children(root), 2u);
        EXPECT_EQ(num_children(left), max_children);
        EXPECT_EQ(num_children(right), max_children - 1);

        EXPECT_EQ(keys(root)[0], max_children * 4);
        EXPECT_EQ(keys(root)[1], max_children * 4 + (max_children - 1) * 5);
        EXPECT_EQ(keys(left).back(), max_children * 4);
        EXPECT_EQ(keys(right).back(), (max_children - 1) * 5);
    }
}
