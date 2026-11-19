"""Regression for the archive admission check, without Docker startup."""

import unittest
from pathlib import Path
from unittest.mock import patch

import phase5a


class Phase6PrepareTest(unittest.TestCase):
    def test_containers2_archive_passes_admission(self):
        root = Path("/opt/bifrost-tests/containers2-abcdef")
        with (
            patch.object(phase5a, "ROOT", root),
            patch.object(phase5a, "command", return_value="") as git,
            patch.object(Path, "stat", return_value=type("Stat", (), {"st_mode": 0o700})()),
            patch.object(phase5a.os, "open", side_effect=FileExistsError) as create,
            self.assertRaises(FileExistsError),
        ):
            phase5a.prepare(phase6=True)
        self.assertEqual(git.call_count, 2)
        create.assert_called_once()
