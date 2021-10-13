#ifndef SNOWBALL_STEMMER_HPP
#define SNOWBALL_STEMMER_HPP

#include <memory>

struct sb_stemmer;

namespace iresearch {

struct stemmer_deleter {
   void operator()(sb_stemmer* p) const noexcept;
};

using stemmer_ptr = std::unique_ptr<sb_stemmer, stemmer_deleter>;

stemmer_ptr make_stemmer_ptr(const char * algorithm, const char * charenc);

} // iresearch

#endif // SNOWBALL_STEMMER_HPP
