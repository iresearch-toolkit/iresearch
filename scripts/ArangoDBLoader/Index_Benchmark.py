################################################################################
## DISCLAIMER
##
## Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
## @author Andrei Lobov
## @author Alexey Bakharew
################################################################################

import sys
import csv
import ctypes
import time
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from globals import *

monthDecode = {
  "JAN":"01", "FEB":"02", "MAR":"03", "APR":"04",
  "MAY":"05", "JUN":"06", "JUL":"07", "AUG":"08",
  "SEP":"09", "OCT":"10", "NOV":"11", "DEC":"12"
} 

def decodeDate(d):
 if len(d) == 24:
   month = d[3:6]
   day = d[0:2]
   year = d[7:11]
   time = d[12:24]
   year += "-";
   year += monthDecode.get(month, "01")
   year += "-"
   year += day
   year += "T"
   year += time
   return year
 return d


############################################
### INDEX
############################################

def do_index_benchmark(arango_instance, filename, line_limit, batch_size):

  # Override csv default 128k field size
  csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))

  # setting for views
  commitIntervalMsec = 2000 
  cleanupIntervalStep = 3
  consolidationIntervalMsec = 1000

  offset = 0
  if len(sys.argv) > 4:
    offset = int(sys.argv[4])

  f = open(filename, mode ='r', encoding='utf-8', errors='replace')
  reader = csv.reader(f, delimiter='\t')
  data = []
  total = 0

  total_time_with_view_Ns = 0
  total_time_without_view_Ns = 0

  count = offset

  index_registry = CollectorRegistry() # index registry for Prometeus https://github.com/iresearch-toolkit/iresearch
  defaultIndexLabelNames = ["engine", "branch", "platform", "batch_size", "doc_count"]
  totalTimeMetric = Gauge('TotalTime', 'Execution time (microseconds)', registry=index_registry, labelnames=defaultIndexLabelNames)
  avgBatchTimeMetric = Gauge('AvgBatchTime', 'Average time for inserting batch', registry=index_registry, labelnames=defaultIndexLabelNames)
  indexingTimeMetric = Gauge('IndexTime', 'Time of indexing entire collection', registry=index_registry, labelnames=defaultIndexLabelNames)


  # this collections already initialized
  wiki_coll_with_index = arango_instance.get_collection(arango_instance.COLLECTION_NAME_WITH_INDEX)
  wiki_coll_without_index = arango_instance.get_collection(arango_instance.COLLECTION_NAME_WITHOUT_INDEX)

  for row in reader:
    if offset > 0:
      offset = offset - 1
      continue
    data.append({'title': row[0].replace("\n", "\\n").replace("\"", "'").replace("\\","/"),
                 'body': row[2].replace("\n", "\\n").replace("\"", "'").replace("\\","/"),
                 'count': count, 'created':decodeDate(row[1])})
    
    if len(data) > batch_size or total == line_limit:

      # collection with view
      # start time
      start = time.perf_counter_ns()
      wiki_coll_with_index.insert_many(data)
      # stop time
      took = (time.perf_counter_ns() - start)
      total_time_with_view_Ns += took
      totalTimeMetric.labels("IResearch", "master", "linux", str(batch_size), str(line_limit)).set(total_time_with_view_Ns / 1000000) # update metric value
      
      wiki_coll_without_index.insert_many(data)

      data.clear()
      avgTime = (total_time_with_view_Ns/ (total/batch_size))/1000000
      avgBatchTimeMetric.labels("IResearch", "master", "linux", str(batch_size), str(line_limit)).set(avgTime) # update metric value

      print('Loaded ' + str(total) + ' ' + str( round((total/line_limit) * 100, 2)) +
            '%  in total ' + str(total_time_with_view_Ns / 1000000) + 'ms Batch:' + 
            str(took/1000000) + 'ms Avg:' + str( avgTime ) + 'ms \n')
    total = total + 1
    if total >= line_limit:
      break
    count = count + 1

  f.close()

  arango_instance.delete_view(arango_instance.VIEW_NAME_AFTER_INSERT)
  # create view 
  start = time.perf_counter_ns()
  arango_instance.get_db().create_arangosearch_view(
      name=arango_instance.VIEW_NAME_AFTER_INSERT,
      properties={"links":{
                  arango_instance.COLLECTION_NAME_WITHOUT_INDEX: {
                    "analyzers": ["identity", "delimiter_analyzer"],
                    "fields":{
                      "body": {}
                    }}}}
      )
  # stop time
  took = (time.perf_counter_ns() - start)
  
  indexingTimeMetric.labels("IResearch", "master", "linux", "-1", str(line_limit)).set(took / 1000000) # update metric value

  # upload index stats to prometheus
  push_to_gateway("http://grafana.arangodb.biz:9091", "ArangoSearch-benchmark-index", registry=index_registry)

