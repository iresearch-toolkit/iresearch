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

#ifndef IRESEARCH_TOKEN_ATTRIBUTES_H
#define IRESEARCH_TOKEN_ATTRIBUTES_H

#include "store/data_input.hpp"

#include "document/serializer.hpp"

#include "index/index_reader.hpp"
#include "index/iterators.hpp"

#include "utils/attributes.hpp"
#include "utils/string.hpp"
#include "utils/type_limits.hpp"
#include "utils/iterator.hpp"

NS_ROOT

//////////////////////////////////////////////////////////////////////////////
/// @class offset 
/// @brief represents token offset in a stream 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API offset : attribute {
  static const uint32_t INVALID_OFFSET = integer_traits< uint32_t >::const_max;

  DECLARE_ATTRIBUTE_TYPE();   
  DECLARE_FACTORY_DEFAULT();

  offset() NOEXCEPT;

  virtual void clear() override {
    start = 0;
    end = 0;
  }

  uint32_t start;
  uint32_t end;
};

//////////////////////////////////////////////////////////////////////////////
/// @class offset 
/// @brief represents token increment in a stream 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API increment : basic_attribute<uint32_t> {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();

  increment() NOEXCEPT;

  virtual void clear() override { value = 1U; }
};

//////////////////////////////////////////////////////////////////////////////
/// @class term_attribute 
/// @brief represents term value in a stream 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API term_attribute : attribute {
  DECLARE_ATTRIBUTE_TYPE();

  term_attribute() NOEXCEPT;

  virtual const bytes_ref& value() const {
    return bytes_ref::nil;
  }

  virtual void clear() { }
};

//////////////////////////////////////////////////////////////////////////////
/// @class term_attribute 
/// @brief represents an arbitrary byte sequence associated with
///        the particular term position in a field
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API payload : basic_attribute<bytes_ref> {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();

  payload() NOEXCEPT;
};

//////////////////////////////////////////////////////////////////////////////
/// @class document 
/// @brief contains a document identifier
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API document: basic_attribute<doc_id_t> {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();

  document() NOEXCEPT;

  virtual void clear() { value = type_limits<type_t::doc_id_t>::invalid(); }
};

//////////////////////////////////////////////////////////////////////////////
/// @class frequency 
/// @brief how many times term appears in a document
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API frequency : basic_attribute<uint64_t> {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();

  frequency() NOEXCEPT;
}; // frequency

//////////////////////////////////////////////////////////////////////////////
/// @class granularity_prefix
/// @brief indexed tokens are prefixed with one byte indicating granularity
///        this is marker attribute only used in field::features and by_range
///        exact values are prefixed with 0
///        the less precise the token the greater its granularity prefix value
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API granularity_prefix: attribute {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();

  granularity_prefix() NOEXCEPT;
  virtual void clear() override {
    // NOOP
  }
}; // granularity_prefix

//////////////////////////////////////////////////////////////////////////////
/// @class norm
/// @brief this is marker attribute only used in field::features in order to
///        allow evaluation of the field normalization factor 
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API norm : attribute, serializer {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();

  FORCE_INLINE static CONSTEXPR float_t empty() {
    return 1.f;
  }

  norm() NOEXCEPT;
  
  virtual bool write(data_output& out) const;

  bool reset(const sub_reader& segment, field_id column, const document& doc);
  float_t read() const;

  virtual void clear() override {
    // NOOP
  }

 private:
  mutable float_t value_;
  sub_reader::value_visitor_f column_;
  columnstore_reader::value_reader_f reader_;
  const document* doc_{};
}; // norm

//////////////////////////////////////////////////////////////////////////////
/// @class term_meta
/// @brief represents metadata associated with the term
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API term_meta : attribute {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();

  term_meta() NOEXCEPT;

  virtual void clear() override {
    docs_count = 0;
  }

  uint64_t docs_count = 0; /* how many documents contain a particular term */
}; // term_meta

//////////////////////////////////////////////////////////////////////////////
/// @class position 
/// @brief represents a term positions in document (iterator)
//////////////////////////////////////////////////////////////////////////////
class IRESEARCH_API position : public attribute {
 public:
  typedef uint32_t value_t;

  class IRESEARCH_API impl : 
      public iterator<value_t>, 
      public util::attributes_provider {
   public:
    DECLARE_PTR(impl);

    impl() = default;
    impl(size_t reserve_attrs);
    virtual void clear() = 0;
    using util::attributes_provider::attributes;
    virtual iresearch::attributes& attributes() override final {
      return attrs_;
    }

   protected:
    iresearch::attributes attrs_;
  };
  
  static const uint32_t INVALID = integer_traits<value_t>::const_max;
  static const uint32_t NO_MORE = INVALID - 1;
  
  DECLARE_CREF(position);
  DECLARE_TYPE_ID(attribute::type_id);
  DECLARE_FACTORY_DEFAULT();

  position() NOEXCEPT;

  virtual void clear() override { /* does nothing */ }

  void prepare(impl* impl) NOEXCEPT { impl_.reset(impl); }  

  bool next() const { return impl_->next(); }

  value_t seek(value_t target) const {
    struct skewed_comparer: std::less<value_t> {
      bool operator()(value_t lhs, value_t rhs) {
        typedef std::less<value_t> Pred;
        return Pred::operator()(1 + lhs, 1 + rhs);
      }
    };

    typedef skewed_comparer pos_less;
    return iresearch::seek(*impl_, target, pos_less()); 
  }

  value_t value() const { return impl_->value(); }

  const iresearch::attributes& attributes() const { 
    return impl_->attributes(); 
  }

  const attribute* get( attribute::type_id id ) const { 
    return impl_->attributes().get(id); 
  }  

  template< typename A > 
  const attribute_ref<A>& get() const {
    return this->attributes().get<A>(); 
  }

 private:
  IRESEARCH_API_PRIVATE_VARIABLES_BEGIN
  mutable impl::ptr impl_;
  IRESEARCH_API_PRIVATE_VARIABLES_END
}; // position

NS_END // ROOT

#endif
