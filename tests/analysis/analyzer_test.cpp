//
// IResearch search engine 
// 
// Copyright © 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "tests_config.hpp"
#include "tests_shared.hpp"
#include "analysis/analyzers.hpp"
#include "utils/runtime_utils.hpp"

NS_BEGIN(tests)

class analyzer_test: public ::testing::Test {

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right before each test).

    // ensure stopwords are loaded/cached for the 'en' locale used for text analysis below
    {
      // same env variable name as iresearch::analysis::text_token_stream::STOPWORD_PATH_ENV_VARIABLE
      const auto text_stopword_path_var = "IRESEARCH_TEXT_STOPWORD_PATH";
      const char* czOldStopwordPath = iresearch::getenv(text_stopword_path_var);
      std::string sOldStopwordPath = czOldStopwordPath == nullptr ? "" : czOldStopwordPath;

      iresearch::setenv(text_stopword_path_var, IResearch_test_resource_dir, true);
      iresearch::analysis::analyzers::get("text", "en"); // stream needed only to load stopwords

      if (czOldStopwordPath) {
        iresearch::setenv(text_stopword_path_var, sOldStopwordPath.c_str(), true);
      }
    }
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right before the destructor).
  }
};

NS_END

using namespace tests;

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

TEST_F(analyzer_test, test_load) {
  {
    auto analyzer = iresearch::analysis::analyzers::get("text", "en");

    ASSERT_NE(nullptr, analyzer);
    ASSERT_TRUE(analyzer->reset("abc"));
  }

  // locale with default ingnored_words
  {
    auto analyzer = iresearch::analysis::analyzers::get("text", "{\"locale\":\"en\"}");

    ASSERT_NE(nullptr, analyzer);
    ASSERT_TRUE(analyzer->reset("abc"));
  }

  // locale with provided ignored_words
  {
    auto analyzer = iresearch::analysis::analyzers::get("text", "{\"locale\":\"en\", \"ignored_words\":[\"abc\", \"def\", \"ghi\"]}");

    ASSERT_NE(nullptr, analyzer);
    ASSERT_TRUE(analyzer->reset("abc"));
  }

  // ...........................................................................
  // invalid
  // ...........................................................................

  // missing required locale
  ASSERT_EQ(nullptr, iresearch::analysis::analyzers::get("text", "{}"));

  // invalid ignored_words
  ASSERT_EQ(nullptr, iresearch::analysis::analyzers::get("text", "{{\"locale\":\"en\", \"ignored_words\":\"abc\"}}"));
  ASSERT_EQ(nullptr, iresearch::analysis::analyzers::get("text", "{{\"locale\":\"en\", \"ignored_words\":[1, 2, 3]}}"));
}
