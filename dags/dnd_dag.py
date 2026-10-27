"""Weekly D&D character generation using exactly eight Airflow tasks."""

import json
import logging
import random
import urllib.request as request
from datetime import datetime, timedelta, timezone
from urllib.error import URLError
from urllib.parse import urljoin

from faker import Faker

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


API_BASE = "https://www.dnd5eapi.co/api/2014/"
CHARACTER_COUNT = 5
COLUMNS = (
    "name", "attributes", "level", "race", "class", "languages",
    "profficiency_choices", "spells",
)
LOGGER = logging.getLogger(__name__)

default_args = {
    "owner": "student",
    "start_date": datetime(2023, 1, 1, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def get_api_json(resource):
    """Read a collection name or a resource URL returned by the API."""
    api_request = request.Request(
        urljoin(API_BASE, resource), headers={"Accept": "application/json"}
    )
    with request.urlopen(api_request, timeout=30) as response:
        return json.load(response)


def generate_base_chars(**kwargs):
    """Python task 1: return five recruits through XCom."""
    fake = Faker()
    characters = []
    for _ in range(CHARACTER_COUNT):
        characters.append({
            "name": fake.name(),
            "attributes": [random.randint(6, 18)] + [
                random.randint(2, 18) for _ in range(5)
            ],
            "level": random.randint(1, 3),
        })
    return characters


def fetch_races_and_classes(**kwargs):
    """Python task 2: assign API races/classes and return an API status."""
    ti = kwargs["ti"]
    characters = ti.xcom_pull(task_ids="generate_base_chars")
    try:
        races = get_api_json("races")["results"]
        classes = get_api_json("classes")["results"]
        if not races or not classes:
            raise ValueError("The API returned no races or classes.")
        for character in characters:
            race = random.choice(races)
            character_class = random.choice(classes)
            character.update({
                "race": race["name"],
                "race_url": race["url"],
                "class": character_class["name"],
                "class_url": character_class["url"],
            })
    except (URLError, OSError, ValueError, KeyError, TypeError) as error:
        LOGGER.exception("Could not obtain D&D races and classes.")
        ti.xcom_push(key="api_error", value=str(error))
        return "FAILED"

    # The return value is a status, so save the character list under its own key.
    ti.xcom_push(key="characters", value=characters)
    return "SUCCESS"


def check_api_success(**kwargs):
    status = kwargs["ti"].xcom_pull(task_ids="fetch_races_and_classes")
    return "fetch_spells_and_profs" if status == "SUCCESS" else "log_error"


def choose_proficiencies(class_data):
    choices = []
    for group in class_data.get("proficiency_choices", []):
        options = [
            option["item"]["name"]
            for option in group.get("from", {}).get("options", [])
            if option.get("option_type") == "reference"
        ]
        choices.extend(random.sample(options, min(group["choose"], len(options))))
    return choices or [item["name"] for item in class_data.get("proficiencies", [])]


def fetch_spells_and_profs(**kwargs):
    """Python task 3: enrich and serialize all eight VARCHAR fields."""
    characters = kwargs["ti"].xcom_pull(
        task_ids="fetch_races_and_classes", key="characters"
    )
    # Reuse API responses for characters sharing a race, class, or level.
    cache = {}

    def cached(resource):
        if resource not in cache:
            cache[resource] = get_api_json(resource)
        return cache[resource]

    for character in characters:
        race_data = cached(character.pop("race_url"))
        class_data = cached(character.pop("class_url"))
        character["languages"] = json.dumps([
            language["name"] for language in race_data.get("languages", [])
        ])
        character["profficiency_choices"] = json.dumps(choose_proficiencies(class_data))
        character["spells"] = "none"

        spellcasting = class_data.get("spellcasting")
        if spellcasting and character["level"] >= spellcasting["level"]:
            class_level = cached(f"{class_data['class_levels']}/{character['level']}")
            slots = class_level.get("spellcasting", {})
            allowed_levels = {
                level for level in (1, 2)
                if slots.get(f"spell_slots_level_{level}", 0) > 0
            }
            if slots.get("cantrips_known", 0) > 0:
                allowed_levels.add(0)
            spells = cached(class_data["spells"])["results"]
            eligible = [spell for spell in spells if spell["level"] in allowed_levels]
            selected = random.sample(eligible, min(character["level"] + 3, len(eligible)))
            character["spells"] = json.dumps([
                {"name": spell["name"], "level": spell["level"]} for spell in selected
            ])

        character["attributes"] = json.dumps(character["attributes"])
        character["level"] = str(character["level"])

    LOGGER.info("Prepared %s characters for PostgreSQL.", len(characters))
    return characters


def log_error(**kwargs):
    """Python task 4: report API failure and mark the run as failed."""
    error = kwargs["ti"].xcom_pull(task_ids="fetch_races_and_classes", key="api_error")
    LOGGER.error("API failed. Cannot generate characters: %s", error)
    raise AirflowException(f"D&D API failed: {error}")


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dnd_characters (
    name VARCHAR,
    attributes VARCHAR,
    level VARCHAR,
    race VARCHAR,
    "class" VARCHAR,
    languages VARCHAR,
    profficiency_choices VARCHAR,
    spells VARCHAR
);
"""
# One parameterized INSERT writes all five rows in the same transaction.
INSERT_SQL = (
    "INSERT INTO dnd_characters ("
    + ", ".join(f'"{column}"' for column in COLUMNS)
    + ") VALUES\n"
    + ",\n".join(
        "(" + ", ".join(f"%({column}_{row})s" for column in COLUMNS) + ")"
        for row in range(CHARACTER_COUNT)
    )
    + ";"
)

with DAG(
    "dnd_weekly_characters",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule="none_failed")

    generate_base_chars_task = PythonOperator(
        task_id="generate_base_chars", python_callable=generate_base_chars
    )
    fetch_races_and_classes_task = PythonOperator(
        task_id="fetch_races_and_classes", python_callable=fetch_races_and_classes
    )
    fetch_spells_and_profs_task = PythonOperator(
        task_id="fetch_spells_and_profs", python_callable=fetch_spells_and_profs
    )
    log_error_task = PythonOperator(
        task_id="log_error", python_callable=log_error, retries=0
    )
    check_api_success_task = BranchPythonOperator(
        task_id="check_api_success", python_callable=check_api_success
    )
    insert_characters_task = PostgresOperator(
        task_id="insert_characters",
        postgres_conn_id="postgres_default",
        sql=CREATE_TABLE_SQL + INSERT_SQL,
        # Keep parameters as a dictionary so the scheduler can load the DAG.
        parameters={
            f"{column}_{row}": (
                "{{ ti.xcom_pull(task_ids='fetch_spells_and_profs')["
                + str(row) + "]['" + column + "'] }}"
            )
            for row in range(CHARACTER_COUNT)
            for column in COLUMNS
        },
    )

    start >> generate_base_chars_task >> fetch_races_and_classes_task >> check_api_success_task
    check_api_success_task >> fetch_spells_and_profs_task >> insert_characters_task >> end
    check_api_success_task >> log_error_task >> end
