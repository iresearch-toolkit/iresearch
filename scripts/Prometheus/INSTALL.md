PythonBenchmark.py script is used to upload IResearch benchmark data to Prometheus.
Upload is done throug pushing data to Prometheus PushGateway. Script requires Python 3.8 or above.

Script run parameters:
python PythonBenchmark.py <Path-to-benchmark-logs> <Platform-used-to-run-benchmark> <Branch-user-to-run-benchmark> <Push-gate-url> <Job-name>

Sample call: python PythonBenchmark.py C:\Working\PythonBenchmark\Data Windows10 master localhost:9091 benchmark

Following metrics are exposed to prometheus:
Time                 Execution time (microseconds)
Memory               Consumed memory (kbytes)
CPU                  CPU utilization % (across all cores, so values more than 100 are expected)
Wall_Clock           Elapsed wall clock (seconds)
MinorPageFaults      Minor (reclaiming a frame) page faults
MajorPageFaults      Major (requiring I/O) page faults
VolContextSwitches   Voluntary context switches
InvolContextSwitches Involuntary context switches

Each metrics comes with folllwong labels (althrough non-applicapable labels contains <None>):
"engine" - Search engine used. IResearch, Lucene. Determined while parsing benchmark logs.
"size"   - Search dataset size. Determined while parsing benchmark logs.
"category" - Benchmarked operation. Determined while parsing benchmark logs.
"repeat"  - number of test repeats. Determined while parsing benchmark logs.
"threads" - number of search/index threads used. Determined while parsing benchmark logs.
"random"  - random mode. Determined while parsing benchmark logs.
"scorer"  - scorer used. Determined while parsing benchmark logs.
"scorerarg" - scorer-arg used. Determined while parsing benchmark logs.
"run" - run number (Each test might be run several times to calculate average results)
"calls" - number of benchmark time measured
"branch" - Git Branch. Taken from <Branch-user-to-run-benchmark>
"platform" - Platform. Taken from <Platform-used-to-run-benchmark> argument
"stage" - Query Execution stage.

In order to configure uploading IResearch benchmark results to prometheus it is needed:

1. Install Prometheus PushGate.
  Docker image prom/pushgateway could be used.
2. Install and configre Prometheus. 
  Docker image prom/prometheus could be used.
  PushGate from step 1 should be added as scrape target.
  ------ From prometheus.yml -----
  scrape_configs:
  - job_name: pushgateway
  honor_timestamps: true
  honor_labels: true
  scrape_interval: 15s
  scrape_timeout: 10s
  metrics_path: /metrics
  scheme: http
  static_configs:
  - targets:
    - 192.168.172.140:9091
  -------- End sample ---------------

3. Optionally Graphana could be configured.
	See Dashboard.json sample for pre-configured dashboard