import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

from azure.cosmos import CosmosClient, exceptions

app = func.FunctionApp()

_cosmos_client = None
_container_client = None


def _get_container_client():
    global _cosmos_client
    global _container_client

    if _container_client is not None:
        return _container_client

    connection_string = os.getenv("COSMOS_CONNECTION_STRING")
    endpoint = os.getenv("COSMOS_ENDPOINT")
    key = os.getenv("COSMOS_KEY")
    database_name = os.getenv("COSMOS_DATABASE_NAME", "leaderboard")
    container_name = os.getenv("COSMOS_CONTAINER_NAME", "players")

    if connection_string:
        _cosmos_client = CosmosClient.from_connection_string(connection_string)
    elif endpoint and key:
        _cosmos_client = CosmosClient(endpoint, key)
    else:
        raise ValueError(
            "Cosmos DB configuration is missing. Set COSMOS_CONNECTION_STRING or COSMOS_ENDPOINT and COSMOS_KEY."
        )

    database = _cosmos_client.get_database_client(database_name)
    _container_client = database.get_container_client(container_name)
    return _container_client


def _require_non_negative_int(payload, field_name):
    value = payload.get(field_name)
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"'{field_name}' must be a non-negative integer")
    return value


def _calculate_match_points(score, kills, deaths, match_duration):
    match_points = score + (kills * 25) - (deaths * 10)

    # Reward flawless games and high kill efficiency.
    if deaths == 0:
        match_points += 100

    if match_duration > 0 and (kills / (match_duration / 60)) >= 0.5:
        match_points += 50

    return max(match_points, 0)


def _rank_from_score(total_score):
    if total_score >= 25000:
        return "Diamond"
    if total_score >= 15000:
        return "Platinum"
    if total_score >= 8000:
        return "Gold"
    if total_score >= 3000:
        return "Silver"
    return "Bronze"


def _get_existing_player_doc(container, player_id):
    query = "SELECT TOP 1 * FROM c WHERE c.playerId = @playerId"
    params = [{"name": "@playerId", "value": player_id}]
    items = list(
        container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
        )
    )
    return items[0] if items else None

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="match-results",
                               connection="repoclonesb_SERVICEBUS") 
def leaderboard_queue_trigger(azservicebus: func.ServiceBusMessage):
    raw_body = azservicebus.get_body().decode("utf-8")
    logging.info("Received Service Bus message: %s", raw_body)

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        logging.exception("Invalid JSON payload")
        return

    try:
        match_id = payload["matchId"]
        player_id = payload["playerId"]
        player_name = payload["playerName"]
        score = _require_non_negative_int(payload, "score")
        kills = _require_non_negative_int(payload, "kills")
        deaths = _require_non_negative_int(payload, "deaths")
        match_duration = _require_non_negative_int(payload, "matchDuration")
    except KeyError as missing:
        logging.exception("Missing required field: %s", missing)
        return
    except ValueError as validation_error:
        logging.exception("Validation error: %s", validation_error)
        return

    try:
        container = _get_container_client()
        existing = _get_existing_player_doc(container, player_id)

        previous_total_score = int((existing or {}).get("totalScore", 0))
        previous_matches = int((existing or {}).get("matchesPlayed", 0))
        previous_kills = int((existing or {}).get("totalKills", 0))
        previous_deaths = int((existing or {}).get("totalDeaths", 0))

        match_points = _calculate_match_points(score, kills, deaths, match_duration)
        updated_total_score = previous_total_score + match_points
        updated_matches = previous_matches + 1
        updated_total_kills = previous_kills + kills
        updated_total_deaths = previous_deaths + deaths

        kdr = round(updated_total_kills / max(updated_total_deaths, 1), 2)
        rank_tier = _rank_from_score(updated_total_score)
        updated_at = datetime.now(timezone.utc).isoformat()

        document = {
            "id": player_id,
            "playerId": player_id,
            "playerName": player_name,
            "lastMatchId": match_id,
            "lastMatchScore": score,
            "lastMatchKills": kills,
            "lastMatchDeaths": deaths,
            "lastMatchDuration": match_duration,
            "matchPoints": match_points,
            "totalScore": updated_total_score,
            "matchesPlayed": updated_matches,
            "totalKills": updated_total_kills,
            "totalDeaths": updated_total_deaths,
            "kdr": kdr,
            "rankTier": rank_tier,
            "updatedAt": updated_at,
        }

        if existing and "createdAt" in existing:
            document["createdAt"] = existing["createdAt"]
        else:
            document["createdAt"] = updated_at

        container.upsert_item(document)
        logging.info(
            "Leaderboard updated for playerId=%s totalScore=%s rankTier=%s",
            player_id,
            updated_total_score,
            rank_tier,
        )
    except exceptions.CosmosHttpResponseError:
        logging.exception("Cosmos DB operation failed for playerId=%s", player_id)
    except Exception:
        logging.exception("Unexpected error while updating leaderboard")


# This example uses SDK types to directly access the underlying ServiceBusReceivedMessage object provided by the Service Bus trigger.
# To use, uncomment the section below and add azurefunctions-extensions-bindings-servicebus to your requirements.txt file
# Ref: aka.ms/functions-sdk-servicebus-python
#
# import azurefunctions.extensions.bindings.servicebus as servicebus
# @app.service_bus_queue_trigger(arg_name="receivedmessage",
#                                queue_name="match-results",
#                                connection="repoclonesb_SERVICEBUS")
# def leaderboard_queue_trigger(receivedmessage: servicebus.ServiceBusReceivedMessage):
#     logging.info("Python ServiceBus queue trigger processed message.")
#     logging.info("Receiving: %s\n"
#                  "Body: %s\n",
#                  receivedmessage,
#                  receivedmessage.body)
