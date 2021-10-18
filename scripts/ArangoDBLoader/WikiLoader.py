# python script for loading IResearch benchmark dump of  Wikipedia into
# ArangoDB database. Uses python-arango driver https://github.com/Joowani/python-arango
# Data is loaded in form { title: 'XXXXX', body: 'XXXXXXXXXXXXX', 'count': XXXX, 'created':XXXX}.
# DB server should be set up to run without authorization 

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
################################################################################

import sys
import os
import csv
import ctypes
import time
from typing import Counter
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

script_path = os.getcwd() + "/ArangoDBLoader/python-arango"

if script_path in sys.path:
    print("oops, it's already in there.")
else:
    sys.path.insert(0, script_path)

from arango import ArangoClient

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

def delete_view(db, view_name):
  for view in db.views():
    if view["name"] == view_name:
      db.delete_view(view_name)
      break
  return


def main():
  if len(sys.argv) < 6:
    print("Usage: host database collection data_file count [offset] Example: python WikiLoader.py 'http://localhost:8529' _system wikipedia benchmark.data 10000000")
    return


  # Override csv default 128k field size
  csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))

  # Initialize the client for ArangoDB.
  client = ArangoClient(hosts=sys.argv[1])
  
  # Upload settings
  filename = sys.argv[4] # data file
  collection = sys.argv[3] # target collection
  database = sys.argv[2]  # target database
  line_limit = int(sys.argv[5]) # how many documents to upload
  batch_size = 1000    # batch size for inserting into Arango

  # setting for views
  commitIntervalMsec = 2000 
  cleanupIntervalStep = 3
  consolidationIntervalMsec = 1000

  offset = 0
  if len(sys.argv) > 6:
    offset = int(sys.argv[6])

  ############################################
  ###INDEX
  ############################################

  # we will create view for this collection before insertion
  collection_index = collection + "_index"

  # we will create view for this collection after insertion
  collection_no_index = collection + "_no_index"

  db = client.db(database)

  if db.has_collection(collection_index):
    db.delete_collection(collection_index)
  wikipedia_index = db.create_collection(collection_index)

  if db.has_collection(collection_no_index):
    db.delete_collection(collection_no_index)
  wikipedia_no_index = db.create_collection(collection_no_index)

  # Create an analyzer.
  db.create_analyzer(
      name="delimiter_analyzer",
      analyzer_type="delimiter",
      properties={ "delimiter": ' ' },
      features=[]
      )

  wiki_index_view = "wiki_index_view"

  # create view for wikipedia_index collection
  delete_view(db, wiki_index_view)

  res = db.create_arangosearch_view(
        name=wiki_index_view,
        properties={"links":{
                    collection_index: {
                      "analyzers": ["identity", "delimiter_analyzer"],
                      "fields":{
                        "body": {}
                      }}}}
        )

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
      wikipedia_index.insert_many(data)
      # stop time
      took = (time.perf_counter_ns() - start)
      total_time_with_view_Ns += took
      totalTimeMetric.labels("IResearch", "master", "linux", str(batch_size), str(line_limit)).set(total_time_with_view_Ns / 1000000) # update metric value
      
      wikipedia_no_index.insert_many(data)

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

  wiki_index_view_after = "wiki_index_view"
  delete_view(db, wiki_index_view_after)
  # create view 
  # start time
  start = time.perf_counter_ns()
  res = db.create_arangosearch_view(
      name=wiki_index_view_after,
      properties={"links":{
                  collection_no_index: {
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


############################################
###SEARCH
############################################

  search_registry = CollectorRegistry() # search registry for Prometeus https://github.com/iresearch-toolkit/iresearch
  defaultSearchLabelNames = ["engine", "branch", "platform", "search_query"]

  memoryUasageMetric = Gauge('MemoryUsage', 'Used memory for searching', registry=search_registry, labelnames=defaultSearchLabelNames)
  ExecTimeMetric = Gauge('ExecutionTime', 'Executed time for searching', registry=search_registry, labelnames=defaultSearchLabelNames)

  # Execute an AQL query which returns a cursor object.
  cursor = db.aql.execute(
    'FOR doc IN {} SEARCH ANALYZER(doc.body == "Feminism", "delimiter_analyzer") return doc'.format(wiki_index_view),
    count=True
  )

  memoryUasageMetric.labels("IResearch", "master", "linux", "search_delimiter").set(cursor.statistics()["peakMemoryUsage"]) # update metric value
  ExecTimeMetric.labels("IResearch", "master", "linux", "search_delimiter").set(cursor.statistics()["execution_time"] * 1000) # update metric value

  # upload search stats to prometheus
  push_to_gateway("http://grafana.arangodb.biz:9091", "ArangoSearch-benchmark-search", registry=search_registry)


if __name__== "__main__":
  main()
