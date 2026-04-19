import json
import logging
import os

import azure.functions as func
from azure.cosmos import CosmosClient, PartitionKey


app = func.FunctionApp()

db_name = os.getenv("COSMOS_DB_DATABASE") or os.getenv("COSMOS_DATABASE_NAME", "gamedb")
container_name = os.getenv("COSMOS_CONTAINER_NAME", "leaderboard")

conn_str = os.getenv("COSMOS_CONNECTION_STRING")
if conn_str:
    cosmos_client = CosmosClient.from_connection_string(conn_str)
else:
    endpoint = os.getenv("COSMOS_DB_ENDPOINT") or os.getenv("COSMOS_ENDPOINT")
    key = os.getenv("COSMOS_DB_KEY") or os.getenv("COSMOS_KEY")
    if not endpoint or not key:
        raise RuntimeError("Set COSMOS_CONNECTION_STRING or COSMOS_ENDPOINT/COSMOS_KEY")
    cosmos_client = CosmosClient(endpoint, credential=key)

database = cosmos_client.create_database_if_not_exists(id=db_name)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path="/playerId"),
)


@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="match-results",
    connection="SERVICE_BUS_CONNECTION",
)
def update_leaderboard(msg: func.ServiceBusMessage) -> None:
    try:
        data = json.loads(msg.get_body().decode("utf-8"))
        item = {
            "id": str(data["playerId"]),
            "playerId": str(data["playerId"]),
            "playerName": data["playerName"],
            "score": data["score"],
        }
        container.upsert_item(item)
        logging.info("Leaderboard updated for playerId=%s", item["playerId"])
    except Exception:
        logging.exception("Failed to update leaderboard")
