import sys
import os

def main():

  # Upload settings
  server=sys.argv[1]
  database = sys.argv[2]  # target database
  collection = sys.argv[3] # target collection
  filename = sys.argv[4] # data file
  line_limit = [100000, 1000000, 10000000] # how many documents to upload
  batch_size = [100, 1000, 10000, 100000]      # batch size for inserting into Arango

  for doc_count in line_limit:
    for batch in batch_size:
      os.system("index_benchmark.py {} {} {} {} {} {}".format(server, database, collection, filename, doc_count, batch))


