////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "tests_shared.hpp"

#include "utils/automaton_utils.hpp"
#include "utils/wildcard_utils.hpp"
#include "draw-impl.h"

TEST(boolean_weight_test, static_const) {
  ASSERT_EQ(fst::kLeftSemiring | fst::kRightSemiring |
            fst::kCommutative | fst::kIdempotent | fst::kPath,
            fst::fsa::BooleanWeight::Properties());
  ASSERT_EQ("boolean", fst::fsa::BooleanWeight::Type());
  ASSERT_EQ(0x3F, fst::fsa::BooleanWeight::PayloadType(fst::fsa::BooleanWeight::MaxPayload));
}

TEST(boolean_weight_test, create) {
  // not a member
  {
    const fst::fsa::BooleanWeight weight;
    ASSERT_FALSE(weight.Member());
    ASSERT_EQ(fst::fsa::BooleanWeight(), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false, 2), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(true), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(true, 1), weight);
    ASSERT_EQ(weight, weight.Quantize());
    ASSERT_EQ(weight, weight.Reverse());
    ASSERT_TRUE(ApproxEqual(weight, weight.Quantize()));
    ASSERT_TRUE(ApproxEqual(weight, weight.Reverse()));
    ASSERT_FALSE(bool(weight));
    ASSERT_NE(fst::fsa::BooleanWeight::One(), weight);
    ASSERT_NE(fst::fsa::BooleanWeight::Zero(), weight);
    ASSERT_NE(true, bool(weight));
    ASSERT_EQ(false, bool(weight));
    ASSERT_EQ(0, weight.Payload());
    ASSERT_EQ(2, weight.Hash());

    {
      std::stringstream ss;
      ss << weight;
      ASSERT_TRUE(ss.str().empty());
    }

    {
      std::stringstream ss;
      weight.Write(ss);

      fst::fsa::BooleanWeight readWeight;
      readWeight.Read(ss);
      ASSERT_EQ(weight, readWeight);
    }
  }

  // Zero
  {
    const fst::fsa::BooleanWeight weight = false;
    ASSERT_TRUE(weight.Member());
    ASSERT_NE(fst::fsa::BooleanWeight(), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(false), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(false, 2), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(true), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(true, 1), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight::NoWeight(), weight.Quantize());
    ASSERT_NE(weight, weight.Quantize());
    ASSERT_EQ(weight, weight.Reverse());
    ASSERT_FALSE(ApproxEqual(weight, weight.Quantize()));
    ASSERT_TRUE(ApproxEqual(weight, weight.Reverse()));
    ASSERT_FALSE(bool(weight));
    ASSERT_NE(fst::fsa::BooleanWeight::One(), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight::Zero(), weight);
    ASSERT_NE(true, bool(weight));
    ASSERT_EQ(false, bool(weight));
    ASSERT_EQ(0, weight.Payload());
    ASSERT_EQ(0, weight.Hash());

    {
      std::stringstream ss;
      ss << weight;
      ASSERT_EQ("{0,0}", ss.str());
    }

    {
      std::stringstream ss;
      weight.Write(ss);

      fst::fsa::BooleanWeight readWeight;
      readWeight.Read(ss);
      ASSERT_EQ(weight, weight);
      ASSERT_EQ(weight, readWeight);
    }
  }

  // Zero + Payload
  {
    const fst::fsa::BooleanWeight weight(false, 25);
    ASSERT_TRUE(weight.Member());
    ASSERT_NE(fst::fsa::BooleanWeight(), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(false), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(false, 2), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(false, 25), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(true), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(true, 1), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(true, 25), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight::NoWeight(), weight.Quantize());
    ASSERT_NE(weight, weight.Quantize());
    ASSERT_EQ(weight, weight.Reverse());
    ASSERT_FALSE(ApproxEqual(weight, weight.Quantize()));
    ASSERT_TRUE(ApproxEqual(weight, weight.Reverse()));
    ASSERT_FALSE(bool(weight));
    ASSERT_NE(fst::fsa::BooleanWeight::One(), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight::Zero(), weight);
    ASSERT_NE(true, bool(weight));
    ASSERT_EQ(false, bool(weight));
    ASSERT_EQ(25, weight.Payload());
    ASSERT_EQ(0, weight.Hash());

    {
      std::stringstream ss;
      ss << weight;
      ASSERT_EQ("{0,25}", ss.str());
    }

    {
      std::stringstream ss;
      weight.Write(ss);

      fst::fsa::BooleanWeight readWeight;
      readWeight.Read(ss);
      ASSERT_EQ(weight, weight);
      ASSERT_EQ(weight, readWeight);
    }
  }

  // One
  {
    const fst::fsa::BooleanWeight weight = true;
    ASSERT_TRUE(weight.Member());
    ASSERT_NE(fst::fsa::BooleanWeight(), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false, 2), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(true), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(true, 1), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight::NoWeight(), weight.Quantize());
    ASSERT_NE(weight, weight.Quantize());
    ASSERT_EQ(weight, weight.Reverse());
    ASSERT_TRUE(bool(weight));
    ASSERT_EQ(fst::fsa::BooleanWeight::One(), weight);
    ASSERT_NE(fst::fsa::BooleanWeight::Zero(), weight);
    ASSERT_EQ(true, bool(weight));
    ASSERT_NE(false, bool(weight));
    ASSERT_EQ(0, weight.Payload());
    ASSERT_EQ(1, weight.Hash());

    {
      std::stringstream ss;
      ss << weight;
      ASSERT_EQ("{1,0}", ss.str());
    }

    {
      std::stringstream ss;
      weight.Write(ss);

      fst::fsa::BooleanWeight readWeight;
      readWeight.Read(ss);
      ASSERT_EQ(weight, weight);
      ASSERT_EQ(weight, readWeight);
    }
  }

  // One + Payload
  {
    const fst::fsa::BooleanWeight weight(true, 32);
    ASSERT_TRUE(weight.Member());
    ASSERT_NE(fst::fsa::BooleanWeight(), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false, 2), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false, 32), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(true), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(true, 1), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(true, 32), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight::NoWeight(), weight.Quantize());
    ASSERT_NE(weight, weight.Quantize());
    ASSERT_EQ(weight, weight.Reverse());
    ASSERT_TRUE(bool(weight));
    ASSERT_EQ(fst::fsa::BooleanWeight::One(), weight);
    ASSERT_NE(fst::fsa::BooleanWeight::Zero(), weight);
    ASSERT_EQ(true, bool(weight));
    ASSERT_NE(false, bool(weight));
    ASSERT_EQ(32, weight.Payload());
    ASSERT_EQ(1, weight.Hash());

    {
      std::stringstream ss;
      ss << weight;
      ASSERT_EQ("{1,32}", ss.str());
    }

    {
      std::stringstream ss;
      weight.Write(ss);

      fst::fsa::BooleanWeight readWeight;
      readWeight.Read(ss);
      ASSERT_EQ(weight, weight);
      ASSERT_EQ(weight, readWeight);
    }
  }

  // Max payload
  {
    const fst::fsa::BooleanWeight weight(true, 0xFF);
    ASSERT_TRUE(weight.Member());
    ASSERT_NE(fst::fsa::BooleanWeight(), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false, 2), weight);
    ASSERT_NE(fst::fsa::BooleanWeight(false, fst::fsa::BooleanWeight::MaxPayload), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(true), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(true, 1), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight(true, fst::fsa::BooleanWeight::MaxPayload), weight);
    ASSERT_EQ(fst::fsa::BooleanWeight::NoWeight(), weight.Quantize());
    ASSERT_NE(weight, weight.Quantize());
    ASSERT_EQ(weight, weight.Reverse());
    ASSERT_TRUE(bool(weight));
    ASSERT_EQ(fst::fsa::BooleanWeight::One(), weight);
    ASSERT_NE(fst::fsa::BooleanWeight::Zero(), weight);
    ASSERT_EQ(true, bool(weight));
    ASSERT_NE(false, bool(weight));
    ASSERT_EQ(fst::fsa::BooleanWeight::PayloadType(fst::fsa::BooleanWeight::MaxPayload),
              weight.Payload());
    ASSERT_EQ(1, weight.Hash());

    {
      std::stringstream ss;
      ss << weight;
      ASSERT_EQ("{1,63}", ss.str());
    }

    {
      std::stringstream ss;
      weight.Write(ss);

      fst::fsa::BooleanWeight readWeight;
      readWeight.Read(ss);
      ASSERT_EQ(weight, weight);
      ASSERT_EQ(weight, readWeight);
    }
  }
}

TEST(boolean_weight_test, divide) {
  using namespace fst;
  using namespace fst::fsa;

  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight(true, 31), BooleanWeight(true, 11), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight(true, 31), BooleanWeight(true, 11), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight(true, 31), BooleanWeight(true, 11), DIVIDE_ANY));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::Zero(), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::Zero(), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::Zero(), DIVIDE_ANY));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::Zero(), BooleanWeight::One(), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::Zero(), BooleanWeight::One(), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::Zero(), BooleanWeight::One(), DIVIDE_ANY));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::Zero(), BooleanWeight::Zero(), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::Zero(), BooleanWeight::Zero(), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::Zero(), BooleanWeight::Zero(), DIVIDE_ANY));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::One(), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::One(), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::One(), DIVIDE_ANY));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::NoWeight(), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::NoWeight(), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::One(), BooleanWeight::NoWeight(), DIVIDE_ANY));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::One(), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::One(), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::One(), DIVIDE_ANY));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::NoWeight(), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::NoWeight(), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::NoWeight(), DIVIDE_ANY));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::One(), DIVIDE_LEFT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::One(), DIVIDE_RIGHT));
  ASSERT_EQ(BooleanWeight::NoWeight(), Divide(BooleanWeight::NoWeight(), BooleanWeight::One(), DIVIDE_ANY));
}

TEST(boolean_weight_test, times) {
  using namespace fst;
  using namespace fst::fsa;

  ASSERT_EQ(BooleanWeight(false, 0), Times(BooleanWeight(false, 31), BooleanWeight(true, 32)));
  ASSERT_EQ(BooleanWeight(true, 11), Times(BooleanWeight(true, 31), BooleanWeight(true, 11)));
  ASSERT_EQ(BooleanWeight(false, 11), Times(BooleanWeight(false, 31), BooleanWeight(false, 11)));
  ASSERT_EQ(BooleanWeight::One(), Times(BooleanWeight::One(), BooleanWeight::One()));
  ASSERT_EQ(BooleanWeight::Zero(), Times(BooleanWeight::One(), BooleanWeight::Zero()));
  ASSERT_EQ(BooleanWeight::One(), Times(BooleanWeight::One(), BooleanWeight::NoWeight()));
  ASSERT_EQ(BooleanWeight::One(), Times(BooleanWeight::NoWeight(), BooleanWeight::NoWeight()));
  ASSERT_EQ(BooleanWeight::One(), Times(BooleanWeight::NoWeight(), BooleanWeight::One()));
}

TEST(boolean_weight_test, plus) {
  using namespace fst;
  using namespace fst::fsa;

  ASSERT_EQ(BooleanWeight(true, 63), Plus(BooleanWeight(false, 31), BooleanWeight(true, 32)));
  ASSERT_EQ(BooleanWeight(true, 31), Plus(BooleanWeight(true, 31), BooleanWeight(true, 11)));
  ASSERT_EQ(BooleanWeight(false, 31), Plus(BooleanWeight(false, 31), BooleanWeight(false, 11)));
  ASSERT_EQ(BooleanWeight::One(), Plus(BooleanWeight::One(), BooleanWeight::One()));
  ASSERT_EQ(BooleanWeight::One(), Plus(BooleanWeight::One(), BooleanWeight::Zero()));
  ASSERT_EQ(BooleanWeight::One(), Plus(BooleanWeight::One(), BooleanWeight::NoWeight()));
  ASSERT_EQ(BooleanWeight::One(), Plus(BooleanWeight::NoWeight(), BooleanWeight::NoWeight()));
  ASSERT_EQ(BooleanWeight::One(), Plus(BooleanWeight::NoWeight(), BooleanWeight::One()));
}

TEST(automaton_test, match_wildcard) {
  // check automaton structure
  {
    auto lhs = irs::from_wildcard<char>("%b%");
    auto rhs = irs::from_wildcard<char>("%b%%%");
    ASSERT_EQ(lhs.NumStates(), rhs.NumStates());

    for (decltype(lhs)::StateId state = 0; state < lhs.NumStates(); ++state) {
      ASSERT_EQ(lhs.NumArcs(state), rhs.NumArcs(state));
    }
  }

  // check automaton structure
  {
    auto lhs = irs::from_wildcard<char>("b%%%%%s");
    auto rhs = irs::from_wildcard<char>("b%%%s");
    ASSERT_EQ(lhs.NumStates(), rhs.NumStates());

    for (decltype(lhs)::StateId state = 0; state < lhs.NumStates(); ++state) {
      ASSERT_EQ(lhs.NumArcs(state), rhs.NumArcs(state));
    }
  }

  // check automaton structure
  {
    auto lhs = irs::from_wildcard<char>("b%%__%%%s%");
    auto rhs = irs::from_wildcard<char>("b%%%%%%%__%%%%%%%%s%");
    ASSERT_EQ(lhs.NumStates(), rhs.NumStates());

    for (decltype(lhs)::StateId state = 0; state < lhs.NumStates(); ++state) {
      ASSERT_EQ(lhs.NumArcs(state), rhs.NumArcs(state));
    }
  }

  // nil string
  {
    auto a = irs::from_wildcard<char>(irs::string_ref::NIL);
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_TRUE(irs::accept<char>(a, ""));
    ASSERT_TRUE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
  }

  // empty string
  {
    auto a = irs::from_wildcard<char>(irs::string_ref::EMPTY);
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_TRUE(irs::accept<char>(a, ""));
    ASSERT_TRUE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
  }

  // any or empty string
  {
    auto a = irs::from_wildcard<char>("%");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_TRUE(irs::accept<char>(a, ""));
    ASSERT_TRUE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "a"));
    ASSERT_TRUE(irs::accept<char>(a, "abc"));
  }

  // any or empty string
  {
    auto a = irs::from_wildcard<char>("%%");

    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_TRUE(irs::accept<char>(a, ""));
    ASSERT_TRUE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "a"));
    ASSERT_TRUE(irs::accept<char>(a, "aa"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d1"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce11d"));
  }

  // any char
  {
    auto a = irs::from_wildcard<char>("_");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "a"));
    ASSERT_FALSE(irs::accept<char>(a, "abc"));
  }

  // two any chars
  {
    auto a = irs::from_wildcard<char>("__");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
    ASSERT_TRUE(irs::accept<char>(a, "ba"));
    ASSERT_FALSE(irs::accept<char>(a, "azbce1d"));
    ASSERT_FALSE(irs::accept<char>(a, "azbce1d1"));
    ASSERT_FALSE(irs::accept<char>(a, "azbce11d"));
  }

  // any char (suffix)
  {
    auto a = irs::from_wildcard<char>("a_");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "a_"));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
    ASSERT_TRUE(irs::accept<char>(a, "ab"));
  }

  // any char (prefix)
  {
    auto a = irs::from_wildcard<char>("_a");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "_a"));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
    ASSERT_TRUE(irs::accept<char>(a, "ba"));
  }

  // escaped '_'
  {
    auto a = irs::from_wildcard<char>("\\_a");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "_a"));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
    ASSERT_FALSE(irs::accept<char>(a, "ba"));
  }

  // escaped '\'
  {
    auto a = irs::from_wildcard<char>("\\\\\\_a");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "\\_a"));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
    ASSERT_FALSE(irs::accept<char>(a, "ba"));
  }

  // nonterminated '\'
  {
    auto a = irs::from_wildcard<char>("a\\");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "a\\"));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
    ASSERT_FALSE(irs::accept<char>(a, "ba"));
  }

  // escaped '%'
  {
    auto a = irs::from_wildcard<char>("\\\\\\%a");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "\\%a"));
    ASSERT_FALSE(irs::accept<char>(a, "a"));
    ASSERT_FALSE(irs::accept<char>(a, "ba"));
  }

  // prefix
  {
    auto a = irs::from_wildcard<char>("foo%");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "foo"));
    ASSERT_TRUE(irs::accept<char>(a, "foobar"));
    ASSERT_FALSE(irs::accept<char>(a, "foa"));
    ASSERT_FALSE(irs::accept<char>(a, "foabar"));
  }

  // prefix
  {
    auto a = irs::from_wildcard<char>("v%%");

    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "vcc"));
    ASSERT_TRUE(irs::accept<char>(a, "vccc"));
    ASSERT_TRUE(irs::accept<char>(a, "vczc"));
    ASSERT_TRUE(irs::accept<char>(a, "vczczvccccc"));
  }

  // suffix
  {
    auto a = irs::from_wildcard<char>("%foo");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "foo"));
    ASSERT_TRUE(irs::accept<char>(a, "bfoo"));
    ASSERT_FALSE(irs::accept<char>(a, "foa"));
    ASSERT_FALSE(irs::accept<char>(a, "bfoa"));
  }

  // mixed
  {
    auto a = irs::from_wildcard<char>("a%bce_d");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d"));
    ASSERT_FALSE(irs::accept<char>(a, "azbce1d1"));
    ASSERT_FALSE(irs::accept<char>(a, "azbce11d"));
  }

  // mixed
  {
    auto a = irs::from_wildcard<char>("%_");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "a"));
    ASSERT_TRUE(irs::accept<char>(a, "aa"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d1"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce11d"));
  }

  // mixed
  {
    auto a = irs::from_wildcard<char>("%%_");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "a"));
    ASSERT_TRUE(irs::accept<char>(a, "aa"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d1"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce11d"));
  }

  // mixed
  {
    auto a = irs::from_wildcard<char>("_%");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "a"));
    ASSERT_TRUE(irs::accept<char>(a, "aa"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce1d1"));
    ASSERT_TRUE(irs::accept<char>(a, "azbce11d"));
  }

  // mixed
  {
    auto a = irs::from_wildcard<char>("v%%c");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "vcc"));
    ASSERT_TRUE(irs::accept<char>(a, "vccc"));
    ASSERT_TRUE(irs::accept<char>(a, "vczc"));
    ASSERT_TRUE(irs::accept<char>(a, "vczczvccccc"));
  }

  // mixed
  {
    auto a = irs::from_wildcard<char>("v%c");
    ASSERT_EQ(fst::kAcceptor | fst::kUnweighted, a.Properties(fst::kAcceptor | fst::kUnweighted, true));
    ASSERT_FALSE(irs::accept<char>(a, ""));
    ASSERT_FALSE(irs::accept<char>(a, irs::string_ref::NIL));
    ASSERT_TRUE(irs::accept<char>(a, "vcc"));
    ASSERT_TRUE(irs::accept<char>(a, "vccc"));
    ASSERT_TRUE(irs::accept<char>(a, "vczc"));
    ASSERT_TRUE(irs::accept<char>(a, "vczczvccccc"));
  }
}

TEST(utf8_transitions_builder_test, ascii) {
  irs::utf8_transitions_builder builder;

  auto assert_arc = [](
      const irs::automaton::Arc& actual_arc,
      irs::automaton::Arc::Label expected_label,
      irs::automaton::StateId expected_target) {
    ASSERT_EQ(expected_label, actual_arc.ilabel);
    ASSERT_EQ(expected_label, actual_arc.olabel);
    ASSERT_EQ(expected_target, actual_arc.nextstate);
    ASSERT_EQ(fst::fsa::BooleanWeight(false), actual_arc.weight);
  };

  static constexpr auto EXPECTED_PROPERTIES =
    fst::kILabelSorted | fst::kOLabelSorted |
    fst::kIDeterministic | fst::kODeterministic |
    fst::kAcceptor;

  // ascii sequence
  {
    irs::automaton a;
    auto start = a.AddState();
    auto finish0 = a.AddState();
    auto finish1 = a.AddState();

    std::vector<std::pair<irs::bytes_ref, irs::automaton::StateId>> arcs;
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("a")), finish0);
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("0")), finish0);
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("7")), finish1);
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("U")), finish0);
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("b")), finish1);
    std::sort(arcs.begin(), arcs.end());

    builder.insert(a, start, fst::kNoStateId, arcs.begin(), arcs.end());

    ASSERT_EQ(EXPECTED_PROPERTIES, a.Properties(EXPECTED_PROPERTIES, true));
    ASSERT_EQ(3, a.NumStates());

    // arcs from start state
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(start, &actual_arcs);
      ASSERT_EQ(arcs.size(), actual_arcs.narcs);

      auto* actual_arc = actual_arcs.arcs;
      for (auto& arc : arcs) {
        ASSERT_EQ(1, arc.first.size());
        assert_arc(*actual_arc, arc.first[0], arc.second);
        ++actual_arc;
      }
      ASSERT_EQ(actual_arc, actual_arcs.arcs + actual_arcs.narcs);
    }

    // arcs from 'finish0'
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(finish0, &actual_arcs);
      ASSERT_EQ(0, actual_arcs.narcs);
    }

    // arcs from 'finish1'
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(finish1, &actual_arcs);
      ASSERT_EQ(0, actual_arcs.narcs);
    }

  }

  // ascii sequence with default state
  {
    irs::automaton a;
    auto start = a.AddState();
    auto finish0 = a.AddState();
    auto finish1 = a.AddState();
    auto intermediate0 = a.NumStates();
    auto intermediate1 = a.NumStates() + 1;
    auto intermediate2 = a.NumStates() + 2;

    a.SetStart(start);

    std::vector<std::pair<irs::bytes_ref, irs::automaton::StateId>> arcs;
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("a")), finish0);
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("0")), finish0);
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("7")), finish1);
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("U")), finish0);
    arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("b")), finish1);
    std::sort(arcs.begin(), arcs.end());

    builder.insert(a, start, finish1, arcs.begin(), arcs.end());

    ASSERT_EQ(EXPECTED_PROPERTIES, a.Properties(EXPECTED_PROPERTIES, true));
    ASSERT_EQ(6, a.NumStates()); // +3 intermediate states

    // arcs from start state
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(start, &actual_arcs);
      ASSERT_EQ(256, actual_arcs.narcs);

      auto* actual_arc = actual_arcs.arcs;
      auto expected_arc = arcs.begin();
      irs::automaton::Arc::Label label = 0;

      while (expected_arc != arcs.end() && label < 192) {
        for (; label < expected_arc->first[0]; ++label) {
          assert_arc(*actual_arc, label, finish1);
          ++actual_arc;
        }

        ASSERT_EQ(label, expected_arc->first[0]);
        assert_arc(*actual_arc, label, expected_arc->second);
        ++label;
        ++actual_arc;
        ++expected_arc;
      }

      for (; label < 192; ++label) {
        assert_arc(*actual_arc, label, finish1);
        ++actual_arc;
      }

      for (; label < 224; ++label) {
        assert_arc(*actual_arc, label, intermediate0);
        ++actual_arc;
      }

      for (; label < 240; ++label) {
        assert_arc(*actual_arc, label, intermediate1);
        ++actual_arc;
      }

      for (; label < 256; ++label) {
        assert_arc(*actual_arc, label, intermediate2);
        ++actual_arc;
      }

      ASSERT_EQ(actual_arc, actual_arcs.arcs + actual_arcs.narcs);
    }

    // arcs from 'intermediate0'
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(intermediate0, &actual_arcs);
      ASSERT_EQ(1, actual_arcs.narcs);
      assert_arc(actual_arcs.arcs[0], fst::fsa::kRho, finish1);
    }

    // arcs from 'intermediate1'
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(intermediate1, &actual_arcs);
      ASSERT_EQ(1, actual_arcs.narcs);
      assert_arc(actual_arcs.arcs[0], fst::fsa::kRho, intermediate0);
    }

    // arcs from 'intermediate2'
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(intermediate2, &actual_arcs);
      ASSERT_EQ(1, actual_arcs.narcs);
      assert_arc(actual_arcs.arcs[0], fst::fsa::kRho, intermediate1);
    }

    // arcs from 'finish0'
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(finish0, &actual_arcs);
      ASSERT_EQ(0, actual_arcs.narcs);
    }

    // arcs from 'finish1'
    {
      fst::ArcIteratorData<irs::automaton::Arc> actual_arcs;
      a.InitArcIterator(finish1, &actual_arcs);
      ASSERT_EQ(0, actual_arcs.narcs);
    }
  }


  //irs::automaton a;
  //auto start = a.AddState();
  //auto invalid = a.AddState();
  //a.SetStart(start);
  //a.SetFinal(finish);

  //std::vector<std::pair<irs::bytes_ref, irs::automaton::StateId>> arcs;
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xF5\x85\x97\x86")), invalid);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xD1\x85")), finish);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xD1\x86")), finish);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("b")), invalid);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xE2\x85\x96")), invalid);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xE2\x9E\x96")), invalid);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xE2\x9E\x96")), invalid);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xE2\x9E\x97")), invalid);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xE3\x9E\x97")), invalid);
  //arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xE3\x85\x97")), invalid);
////  arcs.emplace_back(irs::ref_cast<irs::byte_type>(irs::string_ref("\xF0\x9F\x98\x81")), invalid);
////  arcs.emplace_back(irs::bytes_ref::NIL, def);

  //std::sort(arcs.begin(), arcs.end());

  //irs::utf8_transitions_builder builder(a);
  //builder.insert(start, def, arcs.begin(), arcs.end());

  //fst::drawFst(a, "111");
}

