#include "utils/snowball_stemmer.hpp"

#include "libstemmer.h"

namespace iresearch {


void stemmer_deleter::operator()(sb_stemmer* p) const noexcept {
  sb_stemmer_delete(p);
}

stemmer_ptr make_stemmer_ptr(const char * algorithm, const char * charenc) {
  return std::unique_ptr<sb_stemmer, stemmer_deleter>(sb_stemmer_new(algorithm, charenc));
}

} // iresearch
