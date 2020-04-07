import base64
import json
# from google.cloud import bigquery

project_id = '901492054369'
datasetId = "24617418"
# client = bigquery.Client(project_id)

# def changeLabelsOnSingleTable(datasetId, tableId):
#   dataset = client.get_dataset(datasetId)
#   table_ref = dataset.table(tableId)
#   table = client.get_table(table_ref)
#   print(f"{datasetId}.{tableId} wordt aangepast.")
#   table.labels = dataset.labels
#   table = client.update_table(table, ["labels"])
#   print(f"\t{table.table_id} is updatet!")

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


def run(event, context):
    
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    # changeLabelsOnSingleTable(datasetId)

    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = base64.b64decode(event['data'])


    data = json.loads(pubsub_message)
    message = data["resource"]["labels"]["dataset_id"]
    message2 = data["protoPayload"]["resourceName"].split("/tables/")[1]

    print("Starting the function!")
    import time
    import gspread
    from oauth2client.client import GoogleCredentials
    gc = gspread.authorize(GoogleCredentials.get_application_default())

    print("Reading the sheet:")
    worksheet = gc.open('PubSub Monitor').sheet1

    print("update the sheet")
    worksheet.update_acell("C5", f"DataSet: {message}")
    worksheet.update_acell("C6", f"Table: {message2}")
    
    
