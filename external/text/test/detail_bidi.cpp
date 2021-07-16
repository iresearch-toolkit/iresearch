// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
#include <boost/text/bidirectional.hpp>
#include <boost/text/string_utility.hpp>

#include "generated/bidi_tests.hpp"

#include <gtest/gtest.h>


using namespace boost::text;


// https://unicode.org/reports/tr9/#BD13
TEST(detail_bidi, find_run_sequences_)
{
    using namespace boost::text::detail;

    auto run_used = [](level_run<uint32_t *> r) { return r.used(); };

    // Using bidi_property::L for all portions of the examples called "text".

    {
        props_and_embeddings_t<uint32_t *> paes = {
            // text1
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},

            {0, 0, (uint8_t)bidi_property::RLE, false},

            // text2
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},

            {0, 1, (uint8_t)bidi_property::PDF, false},

            {0, 1, (uint8_t)bidi_property::RLE, false},

            // text3
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},

            {0, 1, (uint8_t)bidi_property::PDF, false},

            // text4
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());

        EXPECT_EQ(runs.size(), 3u);
        EXPECT_TRUE(std::none_of(runs.begin(), runs.end(), run_used));
        EXPECT_EQ(runs[0].begin() - paes.begin(), 0);
        EXPECT_EQ(runs[0].end() - paes.begin(), 4);
        EXPECT_EQ(runs[1].begin() - paes.begin(), 4);
        EXPECT_EQ(runs[1].end() - paes.begin(), 13);
        EXPECT_EQ(runs[2].begin() - paes.begin(), 13);
        EXPECT_EQ(runs[2].end() - paes.begin(), 16);

        auto const run_sequences = find_run_sequences(paes, runs);

        EXPECT_EQ(run_sequences.size(), 3u);
        EXPECT_EQ(run_sequences[0].runs()[0].begin() - paes.begin(), 0);
        EXPECT_EQ(run_sequences[0].runs()[0].end() - paes.begin(), 4);
        EXPECT_EQ(run_sequences[1].runs()[0].begin() - paes.begin(), 4);
        EXPECT_EQ(run_sequences[1].runs()[0].end() - paes.begin(), 13);
        EXPECT_EQ(run_sequences[2].runs()[0].begin() - paes.begin(), 13);
        EXPECT_EQ(run_sequences[2].runs()[0].end() - paes.begin(), 16);
    }

    {
        props_and_embeddings_t<uint32_t *> paes = {
            // text1
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},

            {0, 0, (uint8_t)bidi_property::RLI, false},

            // text2
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},

            {0, 0, (uint8_t)bidi_property::PDI, false},

            {0, 0, (uint8_t)bidi_property::RLI, false},

            // text3
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},

            {0, 0, (uint8_t)bidi_property::PDI, false},

            // text4
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());

        EXPECT_EQ(runs.size(), 5u);
        EXPECT_TRUE(std::none_of(runs.begin(), runs.end(), run_used));
        EXPECT_EQ(runs[0].begin() - paes.begin(), 0);
        EXPECT_EQ(runs[0].end() - paes.begin(), 4);
        EXPECT_EQ(runs[1].begin() - paes.begin(), 4);
        EXPECT_EQ(runs[1].end() - paes.begin(), 7);
        EXPECT_EQ(runs[2].begin() - paes.begin(), 7);
        EXPECT_EQ(runs[2].end() - paes.begin(), 9);
        EXPECT_EQ(runs[3].begin() - paes.begin(), 9);
        EXPECT_EQ(runs[3].end() - paes.begin(), 12);
        EXPECT_EQ(runs[4].begin() - paes.begin(), 12);
        EXPECT_EQ(runs[4].end() - paes.begin(), 16);

        auto const run_sequences = find_run_sequences(paes, runs);

        EXPECT_EQ(run_sequences.size(), 3u);
        EXPECT_EQ(run_sequences[0].runs()[0].begin() - paes.begin(), 0);
        EXPECT_EQ(run_sequences[0].runs()[0].end() - paes.begin(), 4);
        EXPECT_EQ(run_sequences[0].runs()[1].begin() - paes.begin(), 7);
        EXPECT_EQ(run_sequences[0].runs()[1].end() - paes.begin(), 9);
        EXPECT_EQ(run_sequences[0].runs()[2].begin() - paes.begin(), 12);
        EXPECT_EQ(run_sequences[0].runs()[2].end() - paes.begin(), 16);
        EXPECT_EQ(run_sequences[1].runs()[0].begin() - paes.begin(), 4);
        EXPECT_EQ(run_sequences[1].runs()[0].end() - paes.begin(), 7);
        EXPECT_EQ(run_sequences[2].runs()[0].begin() - paes.begin(), 9);
        EXPECT_EQ(run_sequences[2].runs()[0].end() - paes.begin(), 12);
    }

    {
        props_and_embeddings_t<uint32_t *> paes = {
            // text1
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},

            {0, 0, (uint8_t)bidi_property::RLI, false},

            // text2
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},

            {0, 1, (uint8_t)bidi_property::LRI, false},

            // text3
            {0, 2, (uint8_t)bidi_property::L, false},
            {0, 2, (uint8_t)bidi_property::L, false},
            {0, 2, (uint8_t)bidi_property::L, false},

            {0, 2, (uint8_t)bidi_property::RLE, false},

            // text4
            {0, 3, (uint8_t)bidi_property::L, false},
            {0, 3, (uint8_t)bidi_property::L, false},
            {0, 3, (uint8_t)bidi_property::L, false},

            {0, 3, (uint8_t)bidi_property::PDF, false},

            // text5
            {0, 2, (uint8_t)bidi_property::L, false},
            {0, 2, (uint8_t)bidi_property::L, false},
            {0, 2, (uint8_t)bidi_property::L, false},

            {0, 1, (uint8_t)bidi_property::PDI, false},

            // text6
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},
            {0, 1, (uint8_t)bidi_property::L, false},

            {0, 0, (uint8_t)bidi_property::PDI, false},

            // text7
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::L, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());

        EXPECT_EQ(runs.size(), 7u);
        EXPECT_TRUE(std::none_of(runs.begin(), runs.end(), run_used));
        EXPECT_EQ(runs[0].begin() - paes.begin(), 0);
        EXPECT_EQ(runs[0].end() - paes.begin(), 4);
        EXPECT_EQ(runs[1].begin() - paes.begin(), 4);
        EXPECT_EQ(runs[1].end() - paes.begin(), 8);
        EXPECT_EQ(runs[2].begin() - paes.begin(), 8);
        EXPECT_EQ(runs[2].end() - paes.begin(), 12);
        EXPECT_EQ(runs[3].begin() - paes.begin(), 12);
        EXPECT_EQ(runs[3].end() - paes.begin(), 16);
        EXPECT_EQ(runs[4].begin() - paes.begin(), 16);
        EXPECT_EQ(runs[4].end() - paes.begin(), 19);
        EXPECT_EQ(runs[5].begin() - paes.begin(), 19);
        EXPECT_EQ(runs[5].end() - paes.begin(), 23);
        EXPECT_EQ(runs[6].begin() - paes.begin(), 23);
        EXPECT_EQ(runs[6].end() - paes.begin(), 27);

        auto const run_sequences = find_run_sequences(paes, runs);

        EXPECT_EQ(run_sequences.size(), 5u);
        EXPECT_EQ(run_sequences[0].runs()[0].begin() - paes.begin(), 0);
        EXPECT_EQ(run_sequences[0].runs()[0].end() - paes.begin(), 4);
        EXPECT_EQ(run_sequences[0].runs()[1].begin() - paes.begin(), 23);
        EXPECT_EQ(run_sequences[0].runs()[1].end() - paes.begin(), 27);
        EXPECT_EQ(run_sequences[1].runs()[0].begin() - paes.begin(), 4);
        EXPECT_EQ(run_sequences[1].runs()[0].end() - paes.begin(), 8);
        EXPECT_EQ(run_sequences[1].runs()[1].begin() - paes.begin(), 19);
        EXPECT_EQ(run_sequences[1].runs()[1].end() - paes.begin(), 23);
        EXPECT_EQ(run_sequences[2].runs()[0].begin() - paes.begin(), 8);
        EXPECT_EQ(run_sequences[2].runs()[0].end() - paes.begin(), 12);
        EXPECT_EQ(run_sequences[3].runs()[0].begin() - paes.begin(), 12);
        EXPECT_EQ(run_sequences[3].runs()[0].end() - paes.begin(), 16);
        EXPECT_EQ(run_sequences[4].runs()[0].begin() - paes.begin(), 16);
        EXPECT_EQ(run_sequences[4].runs()[0].end() - paes.begin(), 19);
    }
}

TEST(detail_bidi, W1)
{
    using namespace boost::text::detail;

    // W1
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AL, false},
            {0, 0, (uint8_t)bidi_property::NSM, false},
            {0, 0, (uint8_t)bidi_property::NSM, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AL);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AL);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AL);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::NSM, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 1);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::LRI, false},
            {0, 0, (uint8_t)bidi_property::NSM, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::LRI);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::PDI, true},
            {0, 0, (uint8_t)bidi_property::NSM, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::PDI);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AL, false},
            {0, 0, (uint8_t)bidi_property::NSM, false},
            {0, 0, (uint8_t)bidi_property::NSM, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AL);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AL);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AL);
    }
}

TEST(detail_bidi, W2)
{
    using namespace boost::text::detail;

    // W2
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AL, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w2(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AL);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AL, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w2(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AL);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::B);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w2(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::B);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w2(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::B);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::R, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w2(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::B);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
}

TEST(detail_bidi, W4)
{
    using namespace boost::text::detail;

    // W4
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::ES, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w4(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::CS, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w4(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::CS, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w4(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::ES, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w4(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::CS, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w4(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::CS, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w4(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
}

TEST(detail_bidi, W5)
{
    using namespace boost::text::detail;

    // W5
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w5(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w5(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w5(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w5(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ET);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w5(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ET);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w5(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w5(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w5(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
}

TEST(detail_bidi, W6)
{
    using namespace boost::text::detail;

    // W6
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::ES, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::CS, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }

    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::ET, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::ES, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::CS, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::ET, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w6(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 2);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
}

TEST(detail_bidi, W7)
{
    using namespace boost::text::detail;

    // W7
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w7(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::B);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::R, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w7(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::B);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::BN, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w7(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 4);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::BN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::B);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::R, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::BN, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        find_sos_eos(run_sequences, paes, 1); // 1 implies sos==R
        w7(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 4);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::B);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::BN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
}

TEST(detail_bidi, find_bracket_pairs_)
{
    using namespace boost::text::detail;

    {
        uint32_t cps[] = {'a', ')', 'b', '(', 'c'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 0);
    }
    {
        uint32_t cps[] = {'a', ')', 'b', ']', 'c'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 0);
    }
    {
        uint32_t cps[] = {'a', '(', 'b', ')', 'c'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        auto it = bracket_pairs.begin();
        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 1);
        EXPECT_EQ(it->begin().base() - paes.begin(), 1);
        EXPECT_EQ(it->end().base() - paes.begin(), 3);
    }
    {
        uint32_t cps[] = {'a', '(', 'b', '[', 'c', ')', 'd', ']'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
            {cps + 5, 0, (uint8_t)bidi_property::ON, false},
            {cps + 6, 0, (uint8_t)bidi_property::ON, false},
            {cps + 7, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        auto it = bracket_pairs.begin();
        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 1);
        EXPECT_EQ(it->begin().base() - paes.begin(), 1);
        EXPECT_EQ(it->end().base() - paes.begin(), 5);
    }
    {
        uint32_t cps[] = {'a', '(', 'b', ']', 'c', ')', 'd'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
            {cps + 5, 0, (uint8_t)bidi_property::ON, false},
            {cps + 6, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        auto it = bracket_pairs.begin();
        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 1);
        EXPECT_EQ(it->begin().base() - paes.begin(), 1);
        EXPECT_EQ(it->end().base() - paes.begin(), 5);
    }
    {
        uint32_t cps[] = {'a', '(', 'b', ')', 'c', ')', 'd'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
            {cps + 5, 0, (uint8_t)bidi_property::ON, false},
            {cps + 6, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        auto it = bracket_pairs.begin();
        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 1);
        EXPECT_EQ(it->begin().base() - paes.begin(), 1);
        EXPECT_EQ(it->end().base() - paes.begin(), 3);
    }
    {
        uint32_t cps[] = {'a', '(', 'b', '(', 'c', ')', 'd'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
            {cps + 5, 0, (uint8_t)bidi_property::ON, false},
            {cps + 6, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        auto it = bracket_pairs.begin();
        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 1);
        EXPECT_EQ(it->begin().base() - paes.begin(), 3);
        EXPECT_EQ(it->end().base() - paes.begin(), 5);
    }
    {
        uint32_t cps[] = {'a', '(', 'b', '(', 'c', ')', 'd', ')'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
            {cps + 5, 0, (uint8_t)bidi_property::ON, false},
            {cps + 6, 0, (uint8_t)bidi_property::ON, false},
            {cps + 7, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        auto it = bracket_pairs.begin();
        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 2);
        EXPECT_EQ(it->begin().base() - paes.begin(), 1);
        EXPECT_EQ(it->end().base() - paes.begin(), 7);
        ++it;
        EXPECT_EQ(it->begin().base() - paes.begin(), 3);
        EXPECT_EQ(it->end().base() - paes.begin(), 5);
    }
    {
        uint32_t cps[] = {'a', '(', 'b', '{', 'c', '}', 'd', ')'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 0, (uint8_t)bidi_property::ON, false},
            {cps + 1, 0, (uint8_t)bidi_property::ON, false},
            {cps + 2, 0, (uint8_t)bidi_property::ON, false},
            {cps + 3, 0, (uint8_t)bidi_property::ON, false},
            {cps + 4, 0, (uint8_t)bidi_property::ON, false},
            {cps + 5, 0, (uint8_t)bidi_property::ON, false},
            {cps + 6, 0, (uint8_t)bidi_property::ON, false},
            {cps + 7, 0, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);

        auto it = bracket_pairs.begin();
        EXPECT_EQ(std::distance(bracket_pairs.begin(), bracket_pairs.end()), 2);
        EXPECT_EQ(it->begin().base() - paes.begin(), 1);
        EXPECT_EQ(it->end().base() - paes.begin(), 7);
        ++it;
        EXPECT_EQ(it->begin().base() - paes.begin(), 3);
        EXPECT_EQ(it->end().base() - paes.begin(), 5);
    }
}

TEST(detail_bidi, n0_)
{
    using namespace boost::text::detail;

    {
        uint32_t cps[] = {'A', 'B', '(', 'C', 'D', '[', '&', 'e', 'f', ']', '!', ')', 'g', 'h'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 1, (uint8_t)bidi_property::R, false},
            {cps + 1, 1, (uint8_t)bidi_property::R, false},
            {cps + 2, 1, (uint8_t)bidi_property::ON, false},
            {cps + 3, 1, (uint8_t)bidi_property::R, false},
            {cps + 4, 1, (uint8_t)bidi_property::R, false},
            {cps + 5, 1, (uint8_t)bidi_property::ON, false},
            {cps + 6, 1, (uint8_t)bidi_property::ON, false},
            {cps + 7, 1, (uint8_t)bidi_property::L, false},
            {cps + 8, 1, (uint8_t)bidi_property::L, false},
            {cps + 9, 1, (uint8_t)bidi_property::ON, false},
            {cps + 10, 1, (uint8_t)bidi_property::ON, false},
            {cps + 11, 1, (uint8_t)bidi_property::ON, false},
            {cps + 12, 1, (uint8_t)bidi_property::L, false},
            {cps + 13, 1, (uint8_t)bidi_property::L, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);
        n0(run_sequences[0], bracket_pairs);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()),
            14);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::ON);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
    }
    {
        uint32_t cps[] = {'s', 'm', 'i', 't', 'h', ' ', '(', 'f', 'a', 'b', 'r', 'i', 'k', 'a', 'm', ' ', 'A', 'R', 'A', 'B', 'I', 'C', ')', ' ', 'H', 'E', 'B', 'R', 'E', 'W'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 1, (uint8_t)bidi_property::L, false},
            {cps + 1, 1, (uint8_t)bidi_property::L, false},
            {cps + 2, 1, (uint8_t)bidi_property::L, false},
            {cps + 3, 1, (uint8_t)bidi_property::L, false},
            {cps + 4, 1, (uint8_t)bidi_property::L, false},
            {cps + 5, 1, (uint8_t)bidi_property::WS, false},
            {cps + 6, 1, (uint8_t)bidi_property::ON, false},
            {cps + 7, 1, (uint8_t)bidi_property::L, false},
            {cps + 8, 1, (uint8_t)bidi_property::L, false},
            {cps + 9, 1, (uint8_t)bidi_property::L, false},
            {cps + 10, 1, (uint8_t)bidi_property::L, false},
            {cps + 11, 1, (uint8_t)bidi_property::L, false},
            {cps + 12, 1, (uint8_t)bidi_property::L, false},
            {cps + 13, 1, (uint8_t)bidi_property::L, false},
            {cps + 14, 1, (uint8_t)bidi_property::L, false},
            {cps + 15, 1, (uint8_t)bidi_property::WS, false},
            {cps + 16, 1, (uint8_t)bidi_property::R, false},
            {cps + 17, 1, (uint8_t)bidi_property::R, false},
            {cps + 18, 1, (uint8_t)bidi_property::R, false},
            {cps + 19, 1, (uint8_t)bidi_property::R, false},
            {cps + 20, 1, (uint8_t)bidi_property::R, false},
            {cps + 21, 1, (uint8_t)bidi_property::R, false},
            {cps + 22, 1, (uint8_t)bidi_property::ON, false},
            {cps + 23, 1, (uint8_t)bidi_property::WS, false},
            {cps + 24, 1, (uint8_t)bidi_property::R, false},
            {cps + 25, 1, (uint8_t)bidi_property::R, false},
            {cps + 26, 1, (uint8_t)bidi_property::R, false},
            {cps + 27, 1, (uint8_t)bidi_property::R, false},
            {cps + 28, 1, (uint8_t)bidi_property::R, false},
            {cps + 29, 1, (uint8_t)bidi_property::R, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);
        n0(run_sequences[0], bracket_pairs);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()),
            30);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::WS);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::WS);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::WS);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
    }
    {
        uint32_t cps[] = {'s', 'm', 'i', 't', 'h', ' ', '(', 'A', 'R', 'A', 'B', 'I', 'C', ' ', 'f', 'a', 'b', 'r', 'i', 'k', 'a', 'm', ')', ' ', 'H', 'E', 'B', 'R', 'E', 'W'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 1, (uint8_t)bidi_property::L, false},
            {cps + 1, 1, (uint8_t)bidi_property::L, false},
            {cps + 2, 1, (uint8_t)bidi_property::L, false},
            {cps + 3, 1, (uint8_t)bidi_property::L, false},
            {cps + 4, 1, (uint8_t)bidi_property::L, false},
            {cps + 5, 1, (uint8_t)bidi_property::WS, false},
            {cps + 6, 1, (uint8_t)bidi_property::ON, false},
            {cps + 7, 1, (uint8_t)bidi_property::R, false},
            {cps + 8, 1, (uint8_t)bidi_property::R, false},
            {cps + 9, 1, (uint8_t)bidi_property::R, false},
            {cps + 10, 1, (uint8_t)bidi_property::R, false},
            {cps + 11, 1, (uint8_t)bidi_property::R, false},
            {cps + 12, 1, (uint8_t)bidi_property::R, false},
            {cps + 13, 1, (uint8_t)bidi_property::WS, false},
            {cps + 14, 1, (uint8_t)bidi_property::L, false},
            {cps + 15, 1, (uint8_t)bidi_property::L, false},
            {cps + 16, 1, (uint8_t)bidi_property::L, false},
            {cps + 17, 1, (uint8_t)bidi_property::L, false},
            {cps + 18, 1, (uint8_t)bidi_property::L, false},
            {cps + 19, 1, (uint8_t)bidi_property::L, false},
            {cps + 20, 1, (uint8_t)bidi_property::L, false},
            {cps + 21, 1, (uint8_t)bidi_property::L, false},
            {cps + 22, 1, (uint8_t)bidi_property::ON, false},
            {cps + 23, 1, (uint8_t)bidi_property::WS, false},
            {cps + 24, 1, (uint8_t)bidi_property::R, false},
            {cps + 25, 1, (uint8_t)bidi_property::R, false},
            {cps + 26, 1, (uint8_t)bidi_property::R, false},
            {cps + 27, 1, (uint8_t)bidi_property::R, false},
            {cps + 28, 1, (uint8_t)bidi_property::R, false},
            {cps + 29, 1, (uint8_t)bidi_property::R, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);
        n0(run_sequences[0], bracket_pairs);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()),
            30);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::WS);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::WS);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::WS);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
    }
    {
        uint32_t cps[] = {'A', 'R', 'A', 'B', 'I', 'C', ' ', 'b', 'o', 'o', 'k', '(', 's', ')'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps + 0, 1, (uint8_t)bidi_property::R, false},
            {cps + 1, 1, (uint8_t)bidi_property::R, false},
            {cps + 2, 1, (uint8_t)bidi_property::R, false},
            {cps + 3, 1, (uint8_t)bidi_property::R, false},
            {cps + 4, 1, (uint8_t)bidi_property::R, false},
            {cps + 5, 1, (uint8_t)bidi_property::R, false},
            {cps + 6, 1, (uint8_t)bidi_property::WS, false},
            {cps + 7, 1, (uint8_t)bidi_property::L, false},
            {cps + 8, 1, (uint8_t)bidi_property::L, false},
            {cps + 9, 1, (uint8_t)bidi_property::L, false},
            {cps + 10, 1, (uint8_t)bidi_property::L, false},
            {cps + 11, 1, (uint8_t)bidi_property::ON, false},
            {cps + 12, 1, (uint8_t)bidi_property::L, false},
            {cps + 13, 1, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        auto const bracket_pairs = find_bracket_pairs(run_sequences[0]);
        n0(run_sequences[0], bracket_pairs);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()),
            14);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::WS);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
    }
}

TEST(detail_bidi, n1_)
{
    using namespace boost::text::detail;

    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::L, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::L, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::L);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::R, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::R, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::R, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::R, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::R, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::AN, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::R, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::AN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::AN);
    }
    {
        props_and_embeddings_t<uint32_t *> paes = {
            {0, 0, (uint8_t)bidi_property::EN, false},
            {0, 0, (uint8_t)bidi_property::B, false},
            {0, 0, (uint8_t)bidi_property::EN, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto run_sequences = find_run_sequences(paes, runs);
        EXPECT_EQ(run_sequences.size(), 1u);

        n1(run_sequences[0]);

        auto it = run_sequences[0].begin();
        EXPECT_EQ(
            std::distance(run_sequences[0].begin(), run_sequences[0].end()), 3);
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::R);
        ++it;
        EXPECT_EQ(it->prop_, (uint8_t)bidi_property::EN);
    }
}

TEST(detail_bidi, l2_)
{
    using namespace boost::text::detail;

    {
        uint32_t cps_[] = {'c', 'a', 'r', ' ', 'm', 'e', 'a', 'n', 's', ' ', 'C', 'A', 'R', '.'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps_ + 0, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 1, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 2, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 3, 0, (uint8_t)bidi_property::WS, false},
            {cps_ + 4, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 5, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 6, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 7, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 8, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 9, 0, (uint8_t)bidi_property::WS, false},
            {cps_ + 10, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 11, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 12, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 13, 0, (uint8_t)bidi_property::CS, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto reordered_runs = l2(runs);

        std::string result;
        uint32_t cps[1] = {0};
        for (auto run : reordered_runs) {
            if (run.reversed()) {
                for (auto it = run.rbegin(), end = run.rend(); it != end;
                     ++it) {
                    cps[0] = it->cp();
                    result += to_string(cps, cps + 1);
                }
            } else {
                for (auto pae : run) {
                    cps[0] = pae.cp();
                    result += to_string(cps, cps + 1);
                }
            }
        }

        EXPECT_EQ(result, "car means RAC.");
    }
    {
        uint32_t cps_[] = {'<', 'c', 'a', 'r', ' ', 'M', 'E', 'A', 'N', 'S', ' ', 'C', 'A', 'R', '.', '='};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps_ + 0, 0, (uint8_t)bidi_property::RLI, false},
            {cps_ + 1, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 2, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 3, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 4, 1, (uint8_t)bidi_property::WS, false},
            {cps_ + 5, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 6, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 7, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 8, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 9, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 10, 1, (uint8_t)bidi_property::WS, false},
            {cps_ + 11, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 12, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 13, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 14, 1, (uint8_t)bidi_property::CS, false},
            {cps_ + 15, 0, (uint8_t)bidi_property::PDI, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto reordered_runs = l2(runs);

        std::string result;
        uint32_t cps[1] = {0};
        for (auto run : reordered_runs) {
            if (run.reversed()) {
                for (auto it = run.rbegin(), end = run.rend(); it != end;
                     ++it) {
                    cps[0] = it->cp();
                    result += to_string(cps, cps + 1);
                }
            } else {
                for (auto pae : run) {
                    cps[0] = pae.cp();
                    result += to_string(cps, cps + 1);
                }
            }
        }

        EXPECT_EQ(result, "<.RAC SNAEM car=");
    }
    {
        uint32_t cps_[] = {'h', 'e',  ' ',  's',  'a',  'i',  'd',  ' ',  0x201c, '<', 'c', 'a', 'r', ' ', 'M', 'E', 'A', 'N', 'S', ' ', 'C', 'A', 'R', '=', '.', 0x201d, ' ', 0x201c, '<', 'I', 'T', ' ', 'D', 'O', 'E', 'S', '=', ',', 0x201d, ' ', 's', 'h', 'e', ' ', 'a', 'g', 'r', 'e', 'e', 'd', '.'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps_ + 0, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 1, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 2, 0, (uint8_t)bidi_property::WS, false},
            {cps_ + 3, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 4, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 5, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 6, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 7, 0, (uint8_t)bidi_property::WS, false},
            {cps_ + 8, 0, (uint8_t)bidi_property::ON, false}, // ” U+201C Left Double Quotation
            {cps_ + 9, 0, (uint8_t)bidi_property::RLI, false},
            {cps_ + 10, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 11, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 12, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 13, 1, (uint8_t)bidi_property::WS, false},
            {cps_ + 14, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 15, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 16, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 17, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 18, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 19, 1, (uint8_t)bidi_property::WS, false},
            {cps_ + 20, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 21, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 22, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 23, 0, (uint8_t)bidi_property::PDI, false},
            {cps_ + 24, 0, (uint8_t)bidi_property::CS, false},
            {cps_ + 25, 0, (uint8_t)bidi_property::ON, false}, // ” U+201D Right Double Quotation
            {cps_ + 26, 0, (uint8_t)bidi_property::WS, false},
            {cps_ + 27, 0, (uint8_t)bidi_property::ON, false},
            {cps_ + 28, 0, (uint8_t)bidi_property::RLI, false},
            {cps_ + 29, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 30, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 31, 1, (uint8_t)bidi_property::WS, false},
            {cps_ + 32, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 33, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 34, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 35, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 36, 0, (uint8_t)bidi_property::PDI, false},
            {cps_ + 37, 0, (uint8_t)bidi_property::CS, false},
            {cps_ + 38, 0, (uint8_t)bidi_property::ON, false},
            {cps_ + 39, 0, (uint8_t)bidi_property::WS, false},
            {cps_ + 40, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 41, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 42, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 43, 0, (uint8_t)bidi_property::WS, false},
            {cps_ + 44, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 45, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 46, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 47, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 48, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 49, 0, (uint8_t)bidi_property::L, false},
            {cps_ + 50, 0, (uint8_t)bidi_property::CS, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto reordered_runs = l2(runs);

        std::string result;
        uint32_t cps[1] = {0};
        for (auto run : reordered_runs) {
            if (run.reversed()) {
                for (auto it = run.rbegin(), end = run.rend(); it != end;
                     ++it) {
                    cps[0] = it->cp();
                    result += to_string(cps, cps + 1);
                }
            } else {
                for (auto pae : run) {
                    cps[0] = pae.cp();
                    result += to_string(cps, cps + 1);
                }
            }
        }

        EXPECT_EQ(result, std::string((char const *)u8"he said \u201c<RAC SNAEM car=.\u201d \u201c<SEOD TI=,\u201d she agreed."));
    }
    {
        uint32_t cps_[] = {'D', 'I', 'D', ' ', 'Y', 'O', 'U', ' ', 'S', 'A', 'Y', ' ', 0x2019, '>', 'h', 'e', ' ', 's', 'a', 'i', 'd', ' ', 0x201c, '<', 'c', 'a', 'r', ' ', 'M', 'E', 'A', 'N', 'S', ' ', 'C', 'A', 'R', '=', 0x201d, '=', 0x2018, '?'};

        props_and_embeddings_t<uint32_t *> paes = {
            {cps_ + 0, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 1, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 2, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 3, 1, (uint8_t)bidi_property::WS, false},
            {cps_ + 4, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 5, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 6, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 7, 1, (uint8_t)bidi_property::WS, false},
            {cps_ + 8, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 9, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 10, 1, (uint8_t)bidi_property::R, false},
            {cps_ + 11, 1, (uint8_t)bidi_property::WS, false},
            {cps_ + 12, 1, (uint8_t)bidi_property::ON, false}, // U+2018 Single Left Quotation
            {cps_ + 13, 1, (uint8_t)bidi_property::LRI, false},
            {cps_ + 14, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 15, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 16, 2, (uint8_t)bidi_property::WS, false},
            {cps_ + 17, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 18, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 19, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 20, 2, (uint8_t)bidi_property::L, false},
            {cps_ + 21, 2, (uint8_t)bidi_property::WS, false},
            {cps_ + 22, 2, (uint8_t)bidi_property::ON, false},
            {cps_ + 23, 2, (uint8_t)bidi_property::RLI, false},
            {cps_ + 24, 4, (uint8_t)bidi_property::L, false},
            {cps_ + 25, 4, (uint8_t)bidi_property::L, false},
            {cps_ + 26, 4, (uint8_t)bidi_property::L, false},
            {cps_ + 27, 3, (uint8_t)bidi_property::WS, false},
            {cps_ + 28, 3, (uint8_t)bidi_property::R, false},
            {cps_ + 29, 3, (uint8_t)bidi_property::R, false},
            {cps_ + 30, 3, (uint8_t)bidi_property::R, false},
            {cps_ + 31, 3, (uint8_t)bidi_property::R, false},
            {cps_ + 32, 3, (uint8_t)bidi_property::R, false},
            {cps_ + 33, 3, (uint8_t)bidi_property::WS, false},
            {cps_ + 34, 3, (uint8_t)bidi_property::R, false},
            {cps_ + 35, 3, (uint8_t)bidi_property::R, false},
            {cps_ + 36, 3, (uint8_t)bidi_property::R, false},
            {cps_ + 37, 2, (uint8_t)bidi_property::PDI, false},
            {cps_ + 38, 2, (uint8_t)bidi_property::ON, false},
            {cps_ + 39, 1, (uint8_t)bidi_property::PDI, false},
            {cps_ + 40, 1, (uint8_t)bidi_property::ON, false}, // U+2019 Single Right Quotation
            {cps_ + 41, 1, (uint8_t)bidi_property::ON, false},
        };

        auto runs = find_all_runs<uint32_t *>(paes.begin(), paes.end());
        auto reordered_runs = l2(runs);

        std::string result;
        uint32_t cps[1] = {0};
        for (auto run : reordered_runs) {
            if (run.reversed()) {
                for (auto it = run.rbegin(), end = run.rend(); it != end;
                     ++it) {
                    cps[0] = it->cp();
                    result += to_string(cps, cps + 1);
                }
            } else {
                for (auto pae : run) {
                    cps[0] = pae.cp();
                    result += to_string(cps, cps + 1);
                }
            }
        }

        EXPECT_EQ(result, std::string((char const *)u8"?\u2018=he said \u201c<RAC SNAEM car=\u201d>\u2019 YAS UOY DID"));
    }
}
