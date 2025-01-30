import argparse
import csv
import json
import os

from dotenv import load_dotenv
from griptape.artifacts import ListArtifact, TextArtifact
from griptape.drivers import GriptapeCloudEventListenerDriver
from griptape.events import EventBus, EventListener, FinishStructureRunEvent
from griptape.loaders import CsvLoader
from griptape.rules import Rule
from griptape.structures import Agent
from griptape.tasks import PromptTask


def is_running_in_managed_environment() -> bool:
    return "GT_CLOUD_STRUCTURE_RUN_ID" in os.environ


def filter_spreadsheet(input_file, filter_by) -> list:
    with open(input_file, "r") as file:
        first_line = file.readline().strip()
        headers = first_line.split(",")

    agent = Agent(
        tasks=[
            PromptTask(
                input=f"Return the column names related to {filter_by} in the following data: {headers}",
                rules=[Rule("Output a json list of strings")],
            )
        ],
        rules=[
            Rule("Output JUST the data with NO commentary"),
        ],
    )

    agent.run()
    output_json = agent.output.to_text()
    column_names = json.loads(output_json)

    with open(input_file, "r") as file:
        reader = csv.DictReader(file)
        extracted_data = [
            {col: row[col] for col in column_names if col in row} for row in reader
        ]

    return extracted_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_artifacts")  # positional
    parser.add_argument(
        "-d",
        "--data_to_parse",
        required=True,
        help="the prompt data to parse out of the CSV",
    )

    args = parser.parse_args()
    input_data = args.input_artifacts
    filter_by = args.data_to_parse

    load_dotenv()

    output_file_path_local = "./temp_file.csv"
    os.makedirs(os.path.dirname(output_file_path_local), exist_ok=True)
    CsvLoader().save(output_file_path_local, input_data)

    extracted_data = filter_spreadsheet(output_file_path_local, filter_by)

    print(extracted_data)

    if is_running_in_managed_environment():
        artifacts = ListArtifact(extracted_data)

        task_input = TextArtifact(value=None)
        done_event = FinishStructureRunEvent(
            output_task_input=task_input, output_task_output=artifacts
        )
        EventBus.add_event_listener(
            EventListener(event_listener_driver=GriptapeCloudEventListenerDriver())
        )
        EventBus.publish_event(done_event, flush=True)
        print("Published final event")
