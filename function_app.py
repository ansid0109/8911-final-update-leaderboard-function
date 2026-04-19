import json
import logging
import os

import azure.functions as func
from azure.cosmos import CosmosClient


app = func.FunctionApp()

cosmos_client = CosmosClient(
    os.environ["COSMOS_DB_ENDPOINT"],
    credential=os.environ["COSMOS_DB_KEY"],
)
container = cosmos_client.get_database_client(
    os.getenv("COSMOS_DB_DATABASE", "leaderboard-db")
).get_container_client("leaderboard")


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
            "matchId": data["matchId"],
        }
        container.upsert_item(item)
        logging.info("Leaderboard updated for playerId=%s", item["playerId"])
    except Exception:
        logging.exception("Failed to update leaderboard")
