
#!/usr/bin/env python
import re
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import subprocess
import platform
import os
import subprocess
import shutil
import sys
import csv

#Base dictionary labels
baseLabels = ["repeat", "threads", "random", "scorer", "scorer-arg"]


class MetricValue:
  def __init__(self, value, labels):
    self.labels = labels
    self.value = value

class RunFiles:
  """Generic class fo storing benchmark results for all runs on same workset size"""

  def __init__(self, mySize):
    self.size = mySize
    self.timingFiles = []
    self.memoryFiles = []
    self.cpuFiles = []
    self.wallClockFiles = []
    self.pageMinorFaultsFiles = []
    self.pageMajorFaultsFiles = []
    self.voluntaryContextSwitchesFiles = []
    self.involuntaryContextSwitchesFiles = []
    self.labels = {"size": mySize}

  def processTimingFile(self, file, run):
    self.timingFiles.append(self.parseQueriesStats(file, run))

  def processMemoryFile(self, file, run):
    self.memoryFiles.append(self.parseMemoryStats(file, run))
    self.cpuFiles.append(self.parseCPUStats(file, run))
    self.wallClockFiles.append(self.parseWallClockStats(file, run))
    self.pageMinorFaultsFiles.append(self.parseMinorPageFaultStats(file, run))
    self.pageMajorFaultsFiles.append(self.parseMajorPageFaultStats(file, run))

  def parseWallClockStats(self, filename, run):
    metrics = []
    with open(filename, newline='') as datafile:
       for row in datafile:
        result = {"run": int(run)}
        m = re.search("Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): ((([0-9]*):)?([0-9]*):)?(([0-9]*).([0-9]*))", row)  
        if m is not None:
          seconds = 0
          if m.group(3) is not None:
            seconds += int(m.group(3)) * 60 * 60
          if m.group(4) is not None:
            seconds += int(m.group(4)) * 60
          if m.group(6) is not None:
            seconds += int(m.group(6))
          metrics.append(MetricValue(float(seconds), result))
          break
    return metrics

  def parseMinorPageFaultStats(self, filename, run):
    metrics = []
    with open(filename, newline='') as datafile:
      for row in datafile:
       result = {"run": int(run)}
       m = re.search("Minor \(reclaiming a frame\) page faults: ([0-9]*)", row)  
       if m is not None:
         metrics.append(MetricValue(float(m.group(1)), result))
         break
    return metrics

  def parseMajorPageFaultStats(self, filename, run):
    metrics = []
    with open(filename, newline='') as datafile:
      for row in datafile:
       result = {"run": int(run)}
       m = re.search("Major \(requiring I/O\) page faults: ([0-9]*)", row)  
       if m is not None:
         metrics.append(MetricValue(float(m.group(1)), result))
         break
    return metrics

  def parseInvoluntaryContextSwitchesStats(self, filename, run):
    metrics = []
    with open(filename, newline='') as datafile:
      for row in datafile:
       result = {"run": int(run)}
       m = re.search("Involuntary context switches: ([0-9]*)", row)  
       if m is not None:
         metrics.append(MetricValue(float(m.group(1)), result))
         break
    return metrics

  def parseVoluntaryContextSwitchesStats(self, filename, run):
    metrics = []
    with open(filename, newline='') as datafile:
      for row in datafile:
       result = {"run": int(run)}
       m = re.search("Voluntary context switches: ([0-9]*)", row)  
       if m is not None:
         metrics.append(MetricValue(float(m.group(1)), result))
         break
    return metrics

  def parseMemoryStats(self, filename, run):
    metrics = []
    with open(filename, newline='') as datafile:
       for row in datafile:
        result = {"run": int(run)}
        m = re.search("Maximum resident set size \(kbytes\): ([0-9]*)", row)  
        if m is not None:
          metrics.append(MetricValue(float(m.group(1)), result))
          break
    return metrics

  def parseCPUStats(self, filename, run):
    metrics = []
    with open(filename, newline='') as datafile:
       for row in datafile:
        result = {"run": int(run)}
        m = re.search("Percent of CPU this job got: ([0-9]*)%", row)  
        if m is not None:
          metrics.append(MetricValue(float(m.group(1)), result))
          break
    return metrics

  def parseQueriesStats(self, filename, run):
    metrics = []
    with open(filename, newline='') as datafile:
      for row in datafile:
        result = {"run": int(run)}
        m = re.search("Query execution \(([a-zA-Z0-9]*)\) time calls:([0-9]*), time: ([0-9\.\+e]*) us, avg call: ([0-9\.\+e]*) us", row)  
        if m is not None:
           result["stage"] = "Executing"
        else:
           m = re.search("Query building \(([a-zA-Z0-9]*)\) time calls:([0-9]*), time: ([0-9\.\+e]*) us, avg call: ([0-9\.\+e]*) us", row)  
           if m is not None:
             result["stage"] = "Building"
        if m is not None:
          result["category"] = m.group(1)
          result["calls"] = int(m.group(2))
          metrics.append(MetricValue(float(m.group(4)), result))
        else:
          result["stage"] = "General"
          # Could be Index reading or Total time
          m = re.search("Index read time calls:([0-9]*), time: ([0-9\.\+e]*) us, avg call: ([0-9\.\+e]*) us", row)
          if m is not None:
            result["category"] = "IndexRead"
            result["calls"] = int(m.group(1))
            metrics.append(MetricValue(float(m.group(3)), result))
          else:
            m = re.search("Total Time calls:([0-9]*), time: ([0-9\.\+e]*) us, avg call: ([0-9\.\+e]*) us", row)
            if m is not None:
              result["category"] = "Query"
              result["calls"] = int(m.group(1))
              metrics.append(MetricValue(float(m.group(3)), result))
    return metrics



class IResearchRunFiles(RunFiles):
  def __init__(self, mySize):
    super().__init__(mySize)
    self.baseParametersExtracted = False

  def processTimingFile(self, file, run):
    if not self.baseParametersExtracted:
      self.labels.update(self.parseBaseIResearchParameters(file))
      self.baseParametersExtracted = True
    super().processTimingFile(file, run)

  def parseBaseIResearchParameters(self, filename):
    result = {}
    with open(filename, newline='') as csvfile:
      reader = csv.reader(csvfile)
      for row in reader:
        param = row[0].split("=")
        if len(param) == 2 and  param[0] in baseLabels:
          arg = param[0]
          if param[0] == "scorer-arg": # original name is invalid for prometheus
            arg = "scorerarg"
          result[arg] = param[1]
          if len(result) == len(baseLabels):
            break
    return result

class LuceneRunFiles(RunFiles):
  def __init__(self, mySize):
    super().__init__(mySize)
    self.baseParametersExtracted = False

  def parseBaseLuceneParameters(self, filename):
    result = {}
    with open(filename, newline='') as datafile:
      for row in datafile:
        m = re.search("Command being timed: \"java -server -Xms2g -Xmx40g -XX:-TieredCompilation -XX:\+HeapDumpOnOutOfMemoryError -Xbatch -jar .*lucene_search\.jar -dirImpl MMapDirectory -indexPath .*lucene\.data -analyzer StandardAnalyzer -taskSource .*benchmark\.tasks -searchThreadCount ([0-9]*) -taskRepeatCount ([0-9]*) -field body -tasksPerCat 1 -staticSeed -6486775 -seed -6959386 -similarity BM25Similarity -commit multi -hiliteImpl FastVectorHighlighter -log .* -csv -topN 100 -pk\"", row)  
        if m is not None:
          result["threads"] = int(m.group(1))
          result["repeat"] = int(m.group(2))
          break
    return result

  def processMemoryFile(self, file, run):
    if not self.baseParametersExtracted:
      self.labels.update(self.parseBaseLuceneParameters(file))
      self.baseParametersExtracted = True
    super().processMemoryFile(file, run)


def sendStatsToPrometheus(time, memory, cpu, wallClock, parsedFiles, engine):
  # Label must be all present! For start all will be placeholders
  labelsToSendTemplate = {"engine" : "<None>", "size": "<None>", "category": "<None>",\
                        "repeat": "<None>", "threads": "<None>", "random": "<None>",\
                        "scorer": "<None>", "scorerarg": "<None>", "run": "<None>", "calls": "<None>",\
                        "branch": "<None>", "platform": "<None>", "stage": "<None>"}
  for size, stats in parsedFiles.items():
    for s in stats.timingFiles:
      labelsToSendTemplate.update({"engine":engine, "size":size, "platform":sys.argv[2], "branch":sys.argv[3]}) 
      labelsToSendTemplate.update(stats.labels);
      for l in s:
        labelsToSend = labelsToSendTemplate.copy()
        labelsToSend.update(l.labels)
        time.labels(**labelsToSend).set(l.value)
    for s in stats.memoryFiles:
      labelsToSendTemplate.update({"engine":engine, "size":size, "platform":sys.argv[2], "branch":sys.argv[3]}) 
      labelsToSendTemplate.update(stats.labels);
      for l in s:
        labelsToSend = labelsToSendTemplate.copy()
        labelsToSend.update(l.labels)
        memory.labels(**labelsToSend).set(l.value)
    for s in stats.cpuFiles:
      labelsToSendTemplate.update({"engine":engine, "size":size, "platform":sys.argv[2], "branch":sys.argv[3]}) 
      labelsToSendTemplate.update(stats.labels);
      for l in s:
        labelsToSend = labelsToSendTemplate.copy()
        labelsToSend.update(l.labels)
        cpu.labels(**labelsToSend).set(l.value)
    for s in stats.wallClockFiles:
      labelsToSendTemplate.update({"engine":engine, "size":size, "platform":sys.argv[2], "branch":sys.argv[3]}) 
      labelsToSendTemplate.update(stats.labels);
      for l in s:
        labelsToSend = labelsToSendTemplate.copy()
        labelsToSend.update(l.labels)
        wallClock.labels(**labelsToSend).set(l.value)

def main():
  iresearchRunFiles = {}
  luceneRunFiles = {}
  for f in os.listdir(sys.argv[1]):
    m = re.match('(lucene|iresearch)\.(stdout|stdlog|stderr)\.([0-9]*)\.search\.log\.([0-9])', f)
    if m is not None:
      size = int(m.group(3))
      if m.group(1) == "iresearch":
        if size not in iresearchRunFiles.keys():
          iresearchRunFiles[size] = IResearchRunFiles(size)
        if m.group(2) == "stdout":
          iresearchRunFiles[size].processTimingFile(os.path.join(sys.argv[1],f), m.group(4))
        else:
          iresearchRunFiles[size].processMemoryFile(os.path.join(sys.argv[1],f), m.group(4))
      else:
        if size not in luceneRunFiles.keys():
          luceneRunFiles[size] = LuceneRunFiles(size)
        if m.group(2) == "stdlog":
          luceneRunFiles[size].processTimingFile(os.path.join(sys.argv[1],f), m.group(4))
        elif m.group(2) == "stderr":
          luceneRunFiles[size].processMemoryFile(os.path.join(sys.argv[1],f), m.group(4))

  registry = CollectorRegistry()
  time = Gauge('Time', 'Execution time', unit = "us", registry=registry, labelnames=["engine", "size", "category", "repeat", "threads",\
                                                                        "random", "scorer", "scorerarg", "run", "calls",\
                                                                        "branch", "platform", "stage"])
  memory = Gauge('Memory', 'Consumed memory', registry=registry, labelnames=["engine", "size", "category", "repeat", "threads",\
                                                                        "random", "scorer", "scorerarg", "run", "calls",\
                                                                        "branch", "platform", "stage"])
  cpu = Gauge('CPU', 'CPU utilization %', registry=registry, labelnames=["engine", "size", "category", "repeat", "threads",\
                                                                        "random", "scorer", "scorerarg", "run", "calls",\
                                                                        "branch", "platform", "stage"])
  wallClock = Gauge('Wall_Clock', 'Elapsed wall clock', registry=registry, labelnames=["engine", "size", "category", "repeat", "threads",\
                                                                        "random", "scorer", "scorerarg", "run", "calls",\
                                                                        "branch", "platform", "stage"])

  sendStatsToPrometheus(time, memory, cpu, wallClock, iresearchRunFiles, "IResearch")
  sendStatsToPrometheus(time, memory, cpu, wallClock, luceneRunFiles, "Lucene")
  push_to_gateway('localhost:9091', job='benchmark', registry=registry)


if __name__== "__main__":
  main()
