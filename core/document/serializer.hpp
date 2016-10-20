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

#ifndef IRESEARCH_SERIALIZER_H
#define IRESEARCH_SERIALIZER_H

NS_ROOT

struct data_output;

//////////////////////////////////////////////////////////////////////////////
/// @struct serializer 
/// @brief a unified interface for data serialization
//////////////////////////////////////////////////////////////////////////////
struct IRESEARCH_API serializer {

  ////////////////////////////////////////////////////////////////////////////
  /// @brief destructor 
  ////////////////////////////////////////////////////////////////////////////
  virtual ~serializer();

  ////////////////////////////////////////////////////////////////////////////
  /// @brief writers data to output stream
  /// @param out the output stream
  /// @returns true if something has been written
  ////////////////////////////////////////////////////////////////////////////
  virtual bool write(data_output& out) const = 0;
}; // serializer

NS_END

#endif