import base64
import json
from google.cloud import bigquery

project_id = '901492054369'
sheetName = 'PubSub Monitor'
# datasetId = "24617418"
client = bigquery.Client(project_id)

import time
import gspread
from oauth2client.client import GoogleCredentials
gc = gspread.authorize(GoogleCredentials.get_application_default())



def changeLabelsOnSingleTable(datasetId, tableId):
     dataset = client.get_dataset(datasetId)
     table_ref = dataset.table(tableId)
     table = client.get_table(table_ref)
     if table.labels == {}:
          print(f"{datasetId}.{tableId} wordt aangepast.")
          table.labels = dataset.labels
          table = client.update_table(table, ["labels"])
          print(f"\t{table.table_id} is updatet!")
          pushInfoToGsheet(sheetName, f"Dataset: {datasetId}", f"table: {tableId}")
          print("logPushed to Google Sheets")
     else:
          print(f"Tabel ({tableId}) heeft al labels en wordt daarom niet opnieuw gezet!")


# def updateAllNoneLabeledTables(datasetId):
#   # datasetId = "24617418"
#   dataset = client.get_dataset(datasetId)
#   tables = list(client.list_tables(dataset))
#   for table in tables:
#     table_ref = dataset.table(table.table_id)
#     table = client.get_table(table_ref)
#     t_labels = table.labels
#     if t_labels == {}:
#       print(f"Adding labels to {table.table_id}")
#       changeLabelsOnSingleTable(dataset.dataset_id, table.table_id)

def pushInfoToGsheet(sheetName, messageA, messageB=None, cellA="C5", cellB="C6"):

     print("Reading the sheet:")
     worksheet = gc.open(sheetName).sheet1

     print("update the sheet")
     worksheet.update_acell(cellA, f"{messageA}")
     worksheet.update_acell(cellB, f"{messageB}")



def run(event, context):
    print("Starting the function Auto Label BQ Tables!")
    
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    

    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = base64.b64decode(event['data'])


    data = json.loads(pubsub_message)
    datasetId = data["resource"]["labels"]["dataset_id"]
    tableId = data["protoPayload"]["resourceName"].split("/tables/")[1]


       
    changeLabelsOnSingleTable(datasetId, tableId)
    