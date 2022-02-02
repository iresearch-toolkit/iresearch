#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2020 T. Zachary Laine
#
# Distributed under the Boost Software License, Version 1.0. (See
# accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)

test_form = decls = '''\
// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#include <boost/text/normalize_string.hpp>
#include <boost/text/transcode_view.hpp>
#include <boost/text/string_utility.hpp>

#include <gtest/gtest.h>

#include <algorithm>


{0}
'''

idempotence_test_form = decls = '''\
// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#include <boost/text/normalize_string.hpp>
#include <boost/text/transcode_view.hpp>
#include <boost/text/string_utility.hpp>

#include <gtest/gtest.h>

#include <unordered_set>


std::unordered_set<uint32_t> handled_cps = {{{{
{0}
}}}};

TEST(normalization, idempotence)
{{
    for (uint32_t i = 0; i < 0x11000; ++i) {{
        if (handled_cps.count(i))
            continue;
        if (boost::text::surrogate(i))
            continue;

        uint32_t cp[1] = {{i}};
        std::string str = boost::text::to_string(cp, cp + 1);
        std::string const initial_str = str;

        boost::text::normalize<boost::text::nf::c>(str);
        EXPECT_EQ(str, initial_str);

        boost::text::normalize<boost::text::nf::d>(str);
        EXPECT_EQ(str, initial_str);

        boost::text::normalize<boost::text::nf::kd>(str);
        EXPECT_EQ(str, initial_str);

        boost::text::normalize<boost::text::nf::kc>(str);
        EXPECT_EQ(str, initial_str);
    }}
}}
'''

perf_test_form = decls = '''\
// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#include <boost/text/normalize_string.hpp>
#include <boost/text/transcode_view.hpp>
#include <boost/text/string_utility.hpp>

#include <benchmark/benchmark.h>

{0}

BENCHMARK_MAIN()
'''

source = 0
NFC = 1
NFD = 2
NFKC = 3
NFKD = 4

def extract_tests(filename):
    handled_cps = set()

    current_batch = []
    retval = []
    lines = open(filename, 'r').readlines()
    num_lines = 0
    for line in lines:
        if num_lines == 200:
            retval.append(current_batch)
            current_batch = []
            num_lines = 0
        line = line[:-1]
        if not line.startswith('#') and not line.startswith('@') and len(line) != 0:
            comment_start = line.find('#')
            comment = ''
            if comment_start != -1:
                comment = line[comment_start + 1:].strip()
                line = line[:comment_start]
            raw_fields = line.split(';')[:-1]
            if ' ' not in raw_fields[0]:
                handled_cps.add(int(raw_fields[0], 16))
            fields = []
            for f in raw_fields:
                f = map(lambda x: '0x' + x, f.split(' '))
                fields.append(f)
            fields = map(lambda x: map(lambda y: '0x' + y, x.split(' ')), line.split(';'))
            current_batch.append((fields, line, comment))
        num_lines += 1
    if len(current_batch):
        retval.append(current_batch)

    return (retval, handled_cps)

def arrayify(cps, name):
    return 'std::array<uint32_t, {0}> {1} = {{{{ {2} }}}};'.format(
        len(cps), name, ', '.join(cps)
    )

def generate_perf_test(tests):
    normalizations = ['nfc', 'nfd', 'nfkc', 'nfkd']

    test_lines = ''
    for i in range(len(tests)):
        chunk = tests[i]
        all_c1_cps = []
        for elem in chunk:
            (fields, line, comment) = elem
            all_c1_cps += fields[0]
        test_lines += '''{0}
std::string const str_{1:03} = boost::text::to_string(cps_{1:03}.begin(), cps_{1:03}.end());'''.format(arrayify(all_c1_cps, 'const cps_{:03}'.format(i)), i)

        test_lines += '''

void BM_normalize_{0:03}(benchmark::State & state)
{{
'''.format(i)
        for j in range(0, 4):
            test_lines += '    std::string {0}_str;\n'.format(normalizations[j])
        test_lines += '''
    while (state.KeepRunning()) {
        state.PauseTiming();
'''
        for j in range(0, 4):
            test_lines += '        {0}_str = str_{1:03};\n'.format(normalizations[j], i)
        test_lines += '''        state.ResumeTiming();

'''
        for j in range(0, 4):
            test_lines += \
              '        boost::text::normalize<boost::text::nf::{0}>({0}_str);\n'.format(normalizations[j][2:], normalizations[j])
        test_lines += '''    }}
}}
BENCHMARK(BM_normalize_{:03});


'''.format(i)

    cpp_file = open('normalize_perf.cpp'.format(i), 'w')
    cpp_file.write(perf_test_form.format(test_lines, i))

def generate_test_prefix(normalization, chunk_idx, test_idx, line, comment, fields):
    normalized_checks = '\n'
    normalizations = ['nfc', 'nfd', 'nfkc', 'nfkd']
    for f in range(0, 4):
        for i in range(0, 4):
            if fields[f + 1] == fields[i + 1]:
                normalized_checks += \
              '        EXPECT_TRUE(boost::text::normalized<boost::text::nf::{0}>(c{1}.begin(), c{1}.end()));\n'.format(\
                normalizations[i][2:], f + 2)
        normalized_checks += '\n'
    return '''
TEST(normalization, {0}_{1:03}_{2:03})
{{
    // {3}
    // {4}
    {{
        {5}
        {6}
        {7}
        {8}
        {9}
{10}
'''.format(normalization, chunk_idx, test_idx, line, comment, arrayify(fields[0], 'const c1'), arrayify(fields[1], 'const c2'), arrayify(fields[2], 'const c3'), arrayify(fields[3], 'const c4'), arrayify(fields[4], 'const c5'), normalized_checks)

def generate_norm_check(normalization, from_, to_):
    return '''
        {{
            std::string str = boost::text::to_string({1}.begin(), {1}.end());
            boost::text::normalize<boost::text::nf::{0}>(str);
            auto const r = boost::text::as_utf32(str);
            EXPECT_EQ(std::distance(r.begin(), r.end()), (std::ptrdiff_t){2}.size());
            auto {2}_it = {2}.begin();
            int i = 0;
            for (auto x : r) {{
                EXPECT_EQ(x, *{2}_it) << "iteration " << i;
                ++{2}_it;
                ++i;
            }}
        }}
'''.format(normalization[2:], from_, to_)

def generate_nfc_tests(tests):
    for i in range(len(tests)):
        test_lines = ''
        chunk = tests[i]
        test_idx = 0
        for elem in chunk:
            (fields, line, comment) = elem
            test_lines += generate_test_prefix('nfc', i, test_idx, line, comment, fields)
            #    NFC
            #      c2 ==  toNFC(c1) ==  toNFC(c2) ==  toNFC(c3)
            #      c4 ==  toNFC(c4) ==  toNFC(c5)
            test_lines += generate_norm_check('nfc', 'c1', 'c2')
            test_lines += generate_norm_check('nfc', 'c2', 'c2')
            test_lines += generate_norm_check('nfc', 'c3', 'c2')
            test_lines += generate_norm_check('nfc', 'c4', 'c4')
            test_lines += generate_norm_check('nfc', 'c5', 'c4')
            test_lines += '\n    }\n}\n\n'
            test_idx += 1
        cpp_file = open('normalize_to_nfc_{:03}.cpp'.format(i), 'w')
        cpp_file.write(test_form.format(test_lines))

def generate_nfd_tests(tests):
    for i in range(len(tests)):
        test_lines = ''
        chunk = tests[i]
        test_idx = 0
        for elem in chunk:
            (fields, line, comment) = elem
            test_lines += generate_test_prefix('nfd', i, test_idx, line, comment, fields)
            #    NFD
            #      c3 ==  toNFD(c1) ==  toNFD(c2) ==  toNFD(c3)
            #      c5 ==  toNFD(c4) ==  toNFD(c5)
            test_lines += generate_norm_check('nfd', 'c1', 'c3')
            test_lines += generate_norm_check('nfd', 'c2', 'c3')
            test_lines += generate_norm_check('nfd', 'c3', 'c3')
            test_lines += generate_norm_check('nfd', 'c4', 'c5')
            test_lines += generate_norm_check('nfd', 'c5', 'c5')
            test_lines += '\n    }\n}\n\n'
            test_idx += 1
        cpp_file = open('normalize_to_nfd_{:03}.cpp'.format(i), 'w')
        cpp_file.write(test_form.format(test_lines))

def generate_nfkc_tests(tests):
    for i in range(len(tests)):
        test_lines = ''
        chunk = tests[i]
        test_idx = 0
        for elem in chunk:
            (fields, line, comment) = elem
            test_lines += generate_test_prefix('nfkc', i, test_idx, line, comment, fields)
            #    NFKC
            #      c4 == toNFKC(c1) == toNFKC(c2) == toNFKC(c3) == toNFKC(c4) == toNFKC(c5)
            test_lines += generate_norm_check('nfkc', 'c1', 'c4')
            test_lines += generate_norm_check('nfkc', 'c2', 'c4')
            test_lines += generate_norm_check('nfkc', 'c3', 'c4')
            test_lines += generate_norm_check('nfkc', 'c4', 'c4')
            test_lines += generate_norm_check('nfkc', 'c5', 'c4')
            test_lines += '\n    }\n}\n\n'
            test_idx += 1
        cpp_file = open('normalize_to_nfkc_{:03}.cpp'.format(i), 'w')
        cpp_file.write(test_form.format(test_lines))

def generate_nfkd_tests(tests):
    for i in range(len(tests)):
        test_lines = ''
        chunk = tests[i]
        test_idx = 0
        for elem in chunk:
            (fields, line, comment) = elem
            test_lines += generate_test_prefix('nfkd', i, test_idx, line, comment, fields)
            #    NFKD
            #      c5 == toNFKD(c1) == toNFKD(c2) == toNFKD(c3) == toNFKD(c4) == toNFKD(c5)
            test_lines += generate_norm_check('nfkd', 'c1', 'c5')
            test_lines += generate_norm_check('nfkd', 'c2', 'c5')
            test_lines += generate_norm_check('nfkd', 'c3', 'c5')
            test_lines += generate_norm_check('nfkd', 'c4', 'c5')
            test_lines += generate_norm_check('nfkd', 'c5', 'c5')
            test_lines += '\n    }\n}\n\n'
            test_idx += 1
        cpp_file = open('normalize_to_nfkd_{:03}.cpp'.format(i), 'w')
        cpp_file.write(test_form.format(test_lines))

def generate_idempotence_tests(handled_cps):
    handled_cps_lines = map(lambda x: hex(x), handled_cps)
    handled_cps_lines = ', '.join(handled_cps_lines)
    cpp_file = open('normalization_idempotence.cpp', 'w')
    cpp_file.write(idempotence_test_form.format(handled_cps_lines))
    

(tests, handled_cps) = extract_tests('NormalizationTest.txt')

import sys
if '--perf' in sys.argv:
    generate_perf_test(tests)
    exit(0)

generate_nfc_tests(tests)
generate_nfd_tests(tests)
generate_nfkc_tests(tests)
generate_nfkd_tests(tests)
generate_idempotence_tests(handled_cps)
