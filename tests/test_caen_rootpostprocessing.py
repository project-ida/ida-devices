import csv
import importlib.util
import io
import os
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import uproot


def load_postprocessor():
    # Import the script without reading real credentials or connecting to Postgres.
    credentials = ModuleType("psql_credentials")
    for name in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        setattr(credentials, name, "unused-test-value")
    postgres = ModuleType("psycopg2")
    postgres.connect = Mock(side_effect=AssertionError("Unexpected database connection"))
    postgres.OperationalError = type("OperationalError", (Exception,), {})
    postgres.InterfaceError = type("InterfaceError", (Exception,), {})
    extras = ModuleType("psycopg2.extras")
    extras.execute_values = Mock(side_effect=AssertionError("Unexpected database write"))
    postgres.extras = extras
    script = Path(__file__).resolve().parents[1] / "caen-rootpostprocessing.py"
    spec = importlib.util.spec_from_file_location("caen_postprocessing_test", script)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {
        "psql_credentials": credentials,
        "psycopg2": postgres,
        "psycopg2.extras": extras,
    }):
        spec.loader.exec_module(module)
    return module


processor = load_postprocessor()


class PostprocessingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="caen-postprocessing-test-")
        self.addCleanup(temporary.cleanup)
        temporary_root = Path(temporary.name).resolve()
        self.run_folder = temporary_root / "caen" / "DAQ" / "run1"
        self.raw_folder = self.run_folder / "RAW"
        self.raw_folder.mkdir(parents=True)
        self.csv_path = temporary_root / "processed_files.csv"
        self.start_sec = int(datetime(2026, 8, 7, 16, 49, 39, tzinfo=timezone.utc).timestamp())
        self.start_ns = self.start_sec * 1_000_000_000 + 999_999_999
        self.connection = MagicMock()
        self.insert_events = self.enterContext(patch.object(processor, "insert_many_timestamps_to_db"))
        self.insert_metadata = self.enterContext(patch.object(processor, "insert_root_file_to_db"))
        self.enterContext(patch.object(processor, "connect_to_db", return_value=self.connection))
        self.enterContext(patch.object(processor, "csv_path", str(self.csv_path)))
        self.enterContext(patch.object(processor, "get_acquisition_start", return_value=(self.start_sec, self.start_ns)))
        self.output = io.StringIO()
        self.enterContext(redirect_stdout(self.output))

    def make_root_file(self, timestamps, *, name="DataR_CH0@board_0.root", energy_short=None):
        path = self.raw_folder / name
        arrays = {
            "Timestamp": np.array(timestamps, dtype=np.uint64),
            "Energy": np.full(len(timestamps), 100, dtype=np.uint16),
        }
        if energy_short is not None:
            arrays["EnergyShort"] = np.array(energy_short, dtype=np.uint16)
        with uproot.recreate(path) as root_file:
            root_file.mktree("Data_R", {key: value.dtype for key, value in arrays.items()})
            root_file["Data_R"].extend(arrays)
        return path

    def process(self, path):
        return processor.process_root_file(
            str(path), "caen8ch", "0", self.start_sec, self.start_ns, self.connection
        )

    def run_main(self, *, resume=False):
        answers = ["y", "caen8ch", "y", "1"]
        if not resume:
            answers.extend([str(self.run_folder), "0"])
        with patch.dict(os.environ, {"COMPUTER_NAME": "test-pc"}), \
                patch.object(sys, "argv", ["caen-rootpostprocessing.py"]), \
                patch("builtins.input", side_effect=answers):
            processor.main()

    def test_exact_picoseconds_and_second_boundaries(self):
        path = self.make_root_file([0, 999, 1_000, 1_001, 2**63 + 123])
        self.assertTrue(self.process(path)[0])
        rows = self.insert_events.call_args.args[2]
        base = datetime.fromtimestamp(self.start_sec)
        expected = [
            (base + timedelta(microseconds=999_999), 999_999_999_000),
            (base + timedelta(microseconds=999_999), 999_999_999_999),
            (base + timedelta(seconds=1), 0),
            (base + timedelta(seconds=1), 1),
            (datetime.fromtimestamp(self.start_sec + 9_223_373) + timedelta(microseconds=36_854), 36_854_774_931),
        ]
        self.assertEqual([(row[0], row[2]) for row in rows], expected)
        self.assertTrue(all(row[1] == [None, 100.0] for row in rows))
        self.assertTrue(all(type(row[2]) is int for row in rows))

    def test_filename_times_cover_all_chunks_without_rounding(self):
        path = self.make_root_file([0, 999, 3_000_000_000_000, 2_000_000_000_000])
        original_iterate = uproot.behaviors.TTree.TTree.iterate

        def small_chunks(tree, *args, **kwargs):
            kwargs["step_size"] = 2
            return original_iterate(tree, *args, **kwargs)

        with patch.object(uproot.behaviors.TTree.TTree, "iterate", new=small_chunks):
            success, start, end = self.process(path)
        self.assertTrue(success)
        self.assertEqual(self.insert_events.call_count, 2)
        self.assertEqual(start, datetime.fromtimestamp(self.start_sec).strftime("%Y%m%d_%H%M%S"))
        self.assertEqual(end, datetime.fromtimestamp(self.start_sec + 3).strftime("%Y%m%d_%H%M%S"))

    def test_energy_short_still_produces_psp(self):
        path = self.make_root_file([0, 1_000], energy_short=[25, 100])
        self.assertTrue(self.process(path)[0])
        rows = self.insert_events.call_args.args[2]
        self.assertEqual([row[1] for row in rows], [[0.75, 100.0], [0.0, 100.0]])

    def test_empty_tree_does_not_insert_events(self):
        path = self.make_root_file([])
        self.assertEqual(self.process(path), (False, None, None))
        self.insert_events.assert_not_called()

    def test_success_renames_and_records_metadata_and_csv(self):
        original = self.make_root_file([0, 1_000])
        self.run_main()
        renamed = list(self.raw_folder.glob("*.root2"))
        self.assertEqual(len(renamed), 1)
        self.assertFalse(original.exists())
        self.insert_metadata.assert_called_once()
        self.assertEqual(self.insert_metadata.call_args.args[-2:], (renamed[0].name, "caen8ch"))
        saved = processor.read_progress_csv()
        self.assertEqual(saved["filename"].tolist(), [str(renamed[0])])
        self.assertEqual(saved["processed"].tolist(), [True])
        self.assertIn("no failures recorded", self.output.getvalue())
        self.assertIn("stop caen-rootprocessing.py", self.output.getvalue())

    def test_processing_failure_keeps_root_and_records_failed(self):
        original = self.make_root_file([0, 1_000])
        self.insert_events.side_effect = RuntimeError("simulated insert failure")
        self.run_main()
        self.assertTrue(original.exists())
        self.assertEqual(list(self.raw_folder.glob("*.root2")), [])
        self.insert_metadata.assert_not_called()
        self.assertEqual(processor.read_progress_csv()["processed"].tolist(), ["Failed"])
        output = self.output.getvalue()
        self.assertIn("simulated insert failure", output)
        self.assertIn("Total failed files: 1", output)
        self.assertNotIn("no failures recorded", output)
        self.assertNotIn("please delete the CSV", output)

    def test_resume_processes_pending_rows_among_failed_rows(self):
        files = [self.make_root_file([0], name=f"DataR_CH0@board_{index}.root") for index in range(3)]
        with self.csv_path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["filename", "processed"])
            writer.writerows(zip(map(str, files), [True, "Failed", False]))
        self.run_main(resume=True)
        self.insert_events.assert_called_once()
        self.insert_metadata.assert_called_once()
        self.assertEqual(processor.read_progress_csv()["processed"].tolist(), [True, "Failed", True])
        self.assertTrue(files[0].exists())
        self.assertTrue(files[1].exists())
        self.assertFalse(files[2].exists())
        self.assertNotIn("please delete the CSV", self.output.getvalue())


if __name__ == "__main__":
    unittest.main()
