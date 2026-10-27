"""Run from the project root, or stream into Python in the Airflow container."""

import json
from collections import Counter
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch
from urllib.error import URLError

sys.path.insert(0, str(Path.cwd() / "dags"))
import dnd_dag as subject


class CharacterDagTests(unittest.TestCase):
    def test_operator_counts_and_branch_connections(self):
        self.assertEqual(Counter(type(task) for task in subject.dag.tasks), {
            subject.DummyOperator: 2, subject.PythonOperator: 4,
            subject.BranchPythonOperator: 1, subject.PostgresOperator: 1,
        })
        self.assertEqual(subject.dag.get_task("end").trigger_rule, "none_failed")
        self.assertEqual(subject.dag.get_task("end").upstream_task_ids,
                         {"insert_characters", "log_error"})
        self.assertEqual(subject.dag.get_task("check_api_success").downstream_task_ids,
                         {"fetch_spells_and_profs", "log_error"})
        self.assertEqual(subject.dag.schedule_interval, "@weekly")
        self.assertFalse(subject.dag.catchup)

    def test_five_recruits_have_valid_attributes_and_levels(self):
        characters = subject.generate_base_chars()
        self.assertEqual(len(characters), 5)
        for character in characters:
            self.assertTrue(character["name"])
            self.assertEqual(len(character["attributes"]), 6)
            self.assertTrue(6 <= character["attributes"][0] <= 18)
            self.assertTrue(all(2 <= score <= 18 for score in character["attributes"][1:]))
            self.assertIn(character["level"], (1, 2, 3))

    def test_api_success_passes_characters_and_status_separately(self):
        ti = Mock()
        ti.xcom_pull.return_value = subject.generate_base_chars()
        responses = [
            {"results": [{"name": "Elf", "url": "/races/elf"}]},
            {"results": [{"name": "Wizard", "url": "/classes/wizard"}]},
        ]
        with patch.object(subject, "get_api_json", side_effect=responses):
            self.assertEqual(subject.fetch_races_and_classes(ti=ti), "SUCCESS")
        characters = ti.xcom_push.call_args.kwargs["value"]
        self.assertEqual(ti.xcom_push.call_args.kwargs["key"], "characters")
        self.assertTrue(all(char["race"] == "Elf" and char["class"] == "Wizard"
                            for char in characters))

    def test_api_failure_returns_failure_status_and_error(self):
        ti = Mock()
        ti.xcom_pull.return_value = subject.generate_base_chars()
        with patch.object(subject, "get_api_json", side_effect=URLError("offline")):
            with self.assertLogs(subject.LOGGER, level="ERROR"):
                self.assertEqual(subject.fetch_races_and_classes(ti=ti), "FAILED")
        self.assertEqual(ti.xcom_push.call_args.kwargs["key"], "api_error")

    def test_branch_selects_the_correct_task(self):
        ti = Mock()
        for status, expected in (("SUCCESS", "fetch_spells_and_profs"),
                                 ("FAILED", "log_error"), (None, "log_error")):
            ti.xcom_pull.return_value = status
            self.assertEqual(subject.check_api_success(ti=ti), expected)

    def test_enrichment_filters_spells_and_serializes_all_columns(self):
        cases = [("fighter", 1), ("wizard", 1), ("wizard", 3),
                 ("paladin", 1), ("warlock", 3)]
        characters = [{
            "name": f"Recruit {index}", "attributes": [12] * 6,
            "level": level, "race": "Elf", "race_url": "/races/elf",
            "class": character_class, "class_url": f"/classes/{character_class}",
        } for index, (character_class, level) in enumerate(cases)]
        spells = [{"name": f"Spell {index}", "level": level}
                  for index, level in enumerate((0, 1, 1, 2, 3))]

        def api(resource):
            if resource == "/races/elf":
                return {"languages": [{"name": "Common"}, {"name": "Elvish"}]}
            if resource.endswith("/spells"):
                return {"results": spells}
            if "/levels/" in resource:
                level = int(resource.rsplit("/", 1)[1])
                return {"spellcasting": {
                    "cantrips_known": 2, "spell_slots_level_1": 2,
                    "spell_slots_level_2": 2 if level == 3 else 0,
                }}
            data = {
                "class_levels": resource + "/levels", "spells": resource + "/spells",
                "proficiency_choices": [{"choose": 1, "from": {"options": [
                    {"option_type": "reference", "item": {"name": "Arcana"}},
                ]}}],
            }
            if not resource.endswith("fighter"):
                data["spellcasting"] = {"level": 2 if resource.endswith("paladin") else 1}
            return data

        ti = Mock()
        ti.xcom_pull.return_value = characters
        with patch.object(subject, "get_api_json", side_effect=api) as fetch:
            completed = subject.fetch_spells_and_profs(ti=ti)
        self.assertEqual(len(completed), 5)
        self.assertEqual(sum(call.args == ("/races/elf",) for call in fetch.call_args_list), 1)
        for character in completed:
            self.assertEqual(set(character), set(subject.COLUMNS))
            self.assertTrue(all(isinstance(value, str) for value in character.values()))
            self.assertEqual(json.loads(character["languages"]), ["Common", "Elvish"])
            self.assertEqual(json.loads(character["profficiency_choices"]), ["Arcana"])
            if character["class"] in ("fighter", "paladin"):
                self.assertEqual(character["spells"], "none")
            else:
                selected = json.loads(character["spells"])
                self.assertTrue(selected)
                self.assertLessEqual(len(selected), int(character["level"]) + 3)
                maximum = 1 if character["level"] == "1" else 2
                self.assertTrue(all(spell["level"] <= maximum for spell in selected))
                self.assertTrue(all(spell in spells for spell in selected))

    def test_error_task_reports_failure(self):
        ti = Mock()
        ti.xcom_pull.return_value = "offline"
        with self.assertLogs(subject.LOGGER, level="ERROR"):
            with self.assertRaisesRegex(subject.AirflowException, "offline"):
                subject.log_error(ti=ti)


if __name__ == "__main__":
    unittest.main()
