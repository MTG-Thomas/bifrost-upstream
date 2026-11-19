"""Regression for the archive admission check, without Docker startup."""

import unittest
from pathlib import Path
from stat import S_IROTH
from tempfile import TemporaryDirectory
from unittest.mock import patch

import phase5a
from phase6 import mount_private_fixture


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

    def test_private_fixture_can_be_read_by_the_api_user(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "api/scripts"
            scripts.mkdir(parents=True)
            target = scripts / "containers_private_fixture.py"
            target.touch()
            target.chmod(0o600)
            compose = root / "docker-compose.debug.yml"
            compose.write_text("command: uvicorn src.main:app\n")

            mount_private_fixture(root)

            self.assertTrue(target.stat().st_mode & S_IROTH)
            self.assertIn("uvicorn scripts.containers_private_fixture:app", compose.read_text())
