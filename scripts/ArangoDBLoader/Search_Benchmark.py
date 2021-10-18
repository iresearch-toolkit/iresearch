from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from globals import *

############################################
### SEARCH
############################################

def do_search_benchmark(arango_instance):

  search_registry = CollectorRegistry() # search registry for Prometeus https://github.com/iresearch-toolkit/iresearch
  defaultSearchLabelNames = ["engine", "branch", "platform", "search_query"]

  memoryUasageMetric = Gauge('MemoryUsage', 'Used memory for searching', registry=search_registry, labelnames=defaultSearchLabelNames)
  ExecTimeMetric = Gauge('ExecutionTime', 'Executed time for searching', registry=search_registry, labelnames=defaultSearchLabelNames)

  # Execute an AQL query which returns a cursor object.
  cursor = arango_instance.get_db().aql.execute(
    'FOR doc IN {} SEARCH ANALYZER(doc.body == "Feminism", "delimiter_analyzer") return doc'.format(arango_instance.VIEW_NAME_BEFORE_INSERT),
    count=True
  )

  print(cursor.statistics()["peakMemoryUsage"])
  memoryUasageMetric.labels("IResearch", "master", "linux", "search_delimiter").set(cursor.statistics()["peakMemoryUsage"]) # update metric value
  ExecTimeMetric.labels("IResearch", "master", "linux", "search_delimiter").set(cursor.statistics()["execution_time"] * 1000) # update metric value

  # upload search stats to prometheus
  push_to_gateway("http://grafana.arangodb.biz:9091", "ArangoSearch-benchmark-search", registry=search_registry)