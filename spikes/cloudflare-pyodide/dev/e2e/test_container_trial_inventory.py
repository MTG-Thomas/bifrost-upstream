"""Focused shared-account gate and inventory tests for the hosted trial runner."""

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location("combined_rehearsal", Path(__file__).with_name("combined-rehearsal.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.trial = runner.HostedContainers.__new__(runner.HostedContainers)
        self.plan = {"scripts": ["trial-worker"], "application": "trial-app", "registry_repository": "trial-app"}
        self.baseline = {
            "scripts": {"trial-worker-2": "etag"},
            "namespaces": {
                "ns1": ("trial-worker-2", "trial-worker", "Launcher"),
                "ns2": (None, "other", "Launcher"),
            },
            "applications": {"app1": "trial-app-2"},
            "registry": {"trial-app-2": frozenset({"release"})},
        }
        self.trial.provider_snapshot = Mock(return_value=self.baseline)

    def test_foreign_names_and_scriptless_namespace_are_allowed(self):
        self.assertEqual(self.trial.opening_inventory(self.plan), self.baseline)

    def test_exact_identity_collisions_are_refused(self):
        for kind, key, value in (
            ("scripts", "trial-worker", "etag"),
            ("namespaces", "ns3", ("trial-worker", None, "Launcher")),
            ("applications", "app2", "trial-app"),
            ("registry", "trial-app", frozenset({"release"})),
        ):
            with self.subTest(kind=kind):
                self.baseline[kind][key] = value
                with self.assertRaisesRegex(RuntimeError, f"^Trial ownership collision: {kind}$"):
                    self.trial.opening_inventory(self.plan)
                del self.baseline[kind][key]

    def test_inventories_continue_until_the_terminal_page(self):
        cases = (
            (True, {"next_page_token": "opaque"}, {}, "page_token", "opaque"),
            (False, {"page": 1, "total_pages": 2, "total_count": 2}, {"page": 2}, "page", 2),
        )
        for cursor, first, second, param, value in cases:
            with self.subTest(cursor=cursor):
                rows = [{"id": "1"}, {"id": "2"}]
                self.trial.inventory_page = Mock(side_effect=[(rows[:1], first), (rows[1:], second)])
                self.assertEqual(self.trial.inventory("/inventory", cursor=cursor), rows)
                self.assertEqual(self.trial.inventory_page.call_args.args[1][param], value)

    def test_repeated_id_is_an_incomplete_inventory(self):
        row = {"id": "duplicate"}
        self.trial.inventory_page = Mock(side_effect=[([row], {"next_page_token": "opaque"}), ([row], {})])
        with self.assertRaisesRegex(AssertionError, "repeated an ID"):
            self.trial.inventory("/inventory", cursor=True)


if __name__ == "__main__":
    unittest.main()
