import sys
import os

# avoid using 'pip install arango-python' command
arango_path = os.getcwd() + "/../../iresearch.deps/python-arango"
print (arango_path)
if arango_path in sys.path:
    print("oops, it's already in there.")
else:
    sys.path.insert(0, arango_path)
from arango import ArangoClient

class ArangoInstance:

  #####################
  ### CONSTANTS
  #####################

  # we will create view for this collection before insertion
  COLLECTION_NAME_WITH_INDEX = "wikipedia_index"

  # we will create view for this collection after insertion
  COLLECTION_NAME_WITHOUT_INDEX = "wikipedia_no_index"

  # we will create such view before data insertion
  VIEW_NAME_BEFORE_INSERT = "wiki_index_view_before"

  # we will create such view after data insertion
  VIEW_NAME_AFTER_INSERT = "wiki_index_view_after"

  def __init__(self, server, database):
    if len(server) == 0:
      return

    self.server_ = server
    self.database_ = database
    self.db_ =  ArangoClient(server).db(database)

    # create collection 
    self.recreate_collection(self.COLLECTION_NAME_WITH_INDEX)

    # create collection 
    self.recreate_collection(self.COLLECTION_NAME_WITHOUT_INDEX)

    # Create an analyzer.
    self.db_.create_analyzer(
        name="delimiter_analyzer",
        analyzer_type="delimiter",
        properties={ "delimiter": ' ' },
        features=[]
        )

    self.delete_view(self.VIEW_NAME_BEFORE_INSERT)

    self.db_.create_arangosearch_view(
      name=self.VIEW_NAME_BEFORE_INSERT,
      properties={"links":{
                  self.COLLECTION_NAME_WITH_INDEX: {
                    "analyzers": ["identity", "delimiter_analyzer"],
                    "fields":{
                      "body": {}
                    }}}}
      )

      # View 'VIEW_NAME_AFTER_INSERT' will be created later

  #####################
  ### UTILS
  #####################

  # safely delete view from db
  def delete_view(self, view_name):
    for view in self.db_.views():
      if view["name"] == view_name:
        self.db_.delete_view(view_name)
        break
    return

  # safely recreate collection
  def recreate_collection(self, collection_name):
    if self.db_.has_collection(collection_name):
      self.db_.delete_collection(collection_name)
    self.db_.create_collection(collection_name)
  
  def get_db(self):
    return self.db_

  def get_collection(self, name):
    return self.db_.collection(name)
