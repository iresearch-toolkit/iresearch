//
// IResearch search engine 
// 
// Copyright (c) 2016 by EMC Corporation, All Rights Reserved
// 
// This software contains the intellectual property of EMC Corporation or is licensed to
// EMC Corporation from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the License
// Agreement under which it is provided by or on behalf of EMC.
// 

#include "index/directory_reader.hpp"
#include "index/field_meta.hpp"
#include "store/fs_directory.hpp"
#include "analysis/token_attributes.hpp"

#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>

#include <cmdline.h>

NS_LOCAL

namespace fs = boost::filesystem;

const std::string HELP = "help";
const std::string MODE = "mode";
const std::string MODE_DUMP = "dump";
const std::string INDEX_DIR = "index-dir";
const std::string OUTPUT = "out";

NS_END

int dump(const std::string& path, std::ostream& stream) {
  irs::fs_directory dir(path);
  auto reader = irs::directory_reader::open(dir, irs::formats::get("1_0"));

  stream << "Index" 
         << " segmentsCount=" << reader.size()
         << " docsCount=" << reader.docs_count()
         << " liveDocsCount=" << reader.live_docs_count() << std::endl;

  size_t i = 0;
  for (auto& segment : reader) {
    stream << "Segment id=" << i 
           << " docsCount=" << segment.docs_count()
           << " liveDocsCount=" << segment.live_docs_count() << std::endl;

    for (auto fields = segment.fields();fields->next();) {
      auto& field = fields->value();
      auto& meta = field.meta();
      stream << "Field name=" << meta.name
             << " norm=" << meta.norm
             << " minTerm=" << irs::ref_cast<char>(field.min()) 
             << " maxTerm=" << irs::ref_cast<char>(field.max())
             << " termsCount=" << field.size()
             << " docsCount=" << field.docs_count()
             << std::endl;

      auto term = field.iterator();
      auto& term_meta = term->attributes().get<irs::term_meta>();
      stream << "Values" << std::endl;
      for (; term->next(); ) {
        term->read();
        stream << "Term value=" << irs::ref_cast<char>(term->value()) << ""
               << " docsCount=" << term_meta->docs_count
               << std::endl;
      }
    }
    ++i;
  }
  return 0;
}

int dump(const cmdline::parser& args) {
  if (!args.exist(INDEX_DIR)) {
    return 1;
  }

  const auto& path = args.get<std::string>(INDEX_DIR);

  if (path.empty()) {
    return 1;
  }

  if (args.exist(OUTPUT)) {
    const auto& file = args.get<std::string>(OUTPUT);
    std::fstream out(file, std::fstream::out | std::fstream::trunc);
    if (!out) {
      return 1;
    }

    return dump(path, out);
  }

  return dump(path, std::cout);
}

int main(int argc, char* argv[]) {
  // general description
  cmdline::parser cmdroot;
  cmdroot.add(HELP, '?', "Produce help message");
  cmdroot.add<std::string>(MODE, 'm', "Select mode: " + MODE_DUMP, true);

  // dump mode description
  cmdline::parser cmddump;
  cmddump.add<std::string>(INDEX_DIR, 0, "Path to index directory", true);
  cmddump.add<std::string>(OUTPUT, 0, "Output file", false);

  cmdroot.parse(argc, argv);

  if (!cmdroot.exist(MODE) || cmdroot.exist(HELP)) {
    std::cout << cmdroot.usage() << "\n"
              << "Mode " << MODE_DUMP << ":\n"
              << cmddump.usage() << std::endl;
    return 0;
  }

  const auto& mode = cmdroot.get<std::string>(MODE);

  if (MODE_DUMP == mode) {
    // enter dump mode
    cmddump.parse(argc, argv);

    return dump(cmddump);
  }

  return 0;
}
