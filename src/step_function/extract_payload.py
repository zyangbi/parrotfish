import boto3
import json
from typing import Any


def extract_payloads(history: dict[str, Any]) -> dict[str, str]:
    """
    Extract input payloads for each state from the Step Function execution history.
    """

    payloads = {}

    for event in history["events"]:
        if event["type"] == "TaskStateEntered":
            name = event["stateEnteredEventDetails"]["name"]  # name of a state
            payload = json.loads(
                event["stateEnteredEventDetails"]["input"]
            )  # input payload of a state
            payloads[name] = payload

    return payloads


events = boto3.client("stepfunctions").get_execution_history(
    executionArn="arn:aws:states:us-west-2:898429789601:execution:RegressionTuning:036a7707-9251-4b34-88ae-bd833cee32c4"
)
parsed_events = extract_payloads(events)
