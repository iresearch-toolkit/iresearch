#include "tests_shared.hpp"
#include "fasttext.h"

std::vector<float> ftVecToVector(fasttext::Vector &wv) {
    std::vector<float> v{};
    for (auto it = 0; it < wv.size(); it++) {
        v.push_back(*(wv.data() + it));
    }
    return v;
}

TEST(fasttext_model_test, load) {
    auto loc = test_base::resource("cc.en.300.bin");
    auto ft = fasttext::FastText{};
    ft.loadModel(loc);
    ASSERT_EQ(ft.getDimension(), 300);

    fasttext::Vector wv{ft.getDimension()};
    auto word = "happy";
    ft.getWordVector(wv, word);
    auto v = ftVecToVector(wv);
    ASSERT_EQ(v.size(), 300);
}
