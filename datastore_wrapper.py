import json
import traceback
from enum import Enum
from logging import DEBUG, StreamHandler, getLogger

import pandas as pd
import requests
from apiclient.discovery import build
from google.cloud import datastore

logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False


class DatastoreWrapper(object):

  __instance = None

  def __new__(cls, *args, **keys):
    if cls.__instance is None:
      cls.__instance = object.__new__(cls)
    return cls.__instance

  def __init__(self, project_id):
    self.datastore_client = datastore.Client(project_id)

  def query_datastore(self, datastore_kind_name=None, params=None):
    if datastore_kind_name is None or datastore_kind_name == "":
      return None

    query = self.datastore_client.query(kind=datastore_kind_name)
    if params is not None and isinstance(params, dict):
      for k, v in params.items():
        query.add_filter(k, '=', v)

    query_iter = query.fetch()
    result = list(query_iter)
    return result
