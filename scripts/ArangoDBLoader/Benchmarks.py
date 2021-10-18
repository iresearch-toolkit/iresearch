################################################################################
## DISCLAIMER
##
## Copyright 2021 ArangoDB GmbH, Cologne, Germany
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##
## Copyright holder is ArangoDB GmbH, Cologne, Germany
##
## @author Alexey Bakharew
################################################################################

import sys
from globals import *
from Index_Benchmark import do_index_benchmark
from Search_Benchmark import do_search_benchmark

#####################
### MAIN
#####################

def main():
  # Upload settings
  server = sys.argv[1] # target server
  database = sys.argv[2] # target database
  filename = sys.argv[3] # data file

  line_limit = [100000] # how many documents to upload
  batch_size = [1000]  # batch size for inserting into Arango

  arango_instance = ArangoInstance(server, database)

  for doc_count in line_limit:
    for batch in batch_size:
      #do_index_benchmark(arango_instance, filename, doc_count, batch)
      do_search_benchmark(arango_instance)

if __name__== "__main__":
  main()