"""Focused shared-account gate and inventory tests for the hosted trial runner."""

import importlib.util
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("combined_rehearsal", Path(__file__).with_name("combined-rehearsal.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
spec = importlib.util.spec_from_file_location("container_trial_planner", Path(__file__).with_name("container-trial-planner.py"))
planner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(planner)


class PlannerTests(unittest.TestCase):
    def test_reads_destination_from_a_product_class_method(self):
        class Registry:
            async def copy(self, account):
                destination = f"registry.example.test/{account}/repository:release"
                return destination

        value = planner.assignment_from(Registry.copy, "destination")
        self.assertEqual(planner.render(value, account="test"), "registry.example.test/test/repository:release")


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.trial = runner.HostedContainers.__new__(runner.HostedContainers)
        self.trial.account = "fixture"
        self.plan = {"scripts": ["trial-worker"], "application": "trial-app", "registry_repository": "trial-app"}
        self.baseline = {
            "scripts": {"trial-worker-2": "etag"},
            "namespaces": {
                "ns1": ("trial-worker-2", "trial-worker", "Launcher"),
            },
            "applications": {"app1": "trial-app-2"},
            "registry": {"trial-app-2": frozenset({"release"})},
        }
        self.trial.provider_snapshot = Mock(return_value=self.baseline)
        self.trial.worker_receipts = Mock(return_value=[])

    def test_foreign_near_misses_are_allowed(self):
        self.assertEqual(self.trial.opening_inventory(self.plan), self.baseline)

    def test_unattributed_foreign_namespace_is_supported_by_rc03(self):
        self.baseline["namespaces"]["detached"] = (None, "other", "Launcher")
        self.assertEqual(self.trial.opening_inventory(self.plan), self.baseline)

    def test_snapshot_accepts_captured_provider_shapes(self):
        namespace_rows = [
            {"id": f"ns-{index}", "class": "Launcher", "name": "foreign", "script": "foreign-worker"}
            for index in range(23)
        ]
        applications = [{"id": f"app-{index}", "name": f"foreign-app-{index}"} for index in range(5)]
        registry = [{"name": "foreign-image", "tags": ["release"]}]
        envelopes = [
            {"success": True, "result": [{"id": "foreign-worker"}]},
            {"success": True, "result": namespace_rows[:20],
             "result_info": {"page": 1, "per_page": 20, "count": 20, "total_count": 23}},
            {"success": True, "result": namespace_rows[20:],
             "result_info": {"page": 2, "per_page": 20, "count": 3, "total_count": 23}},
            {"success": True, "result": applications},
        ]
        self.trial.provider_origin = "https://provider.example.test"
        self.trial.cfg = {"managementToken": "fixture"}
        self.trial.image_probe = Mock(return_value=registry)
        with patch.object(runner, "request_json", side_effect=[(200, body) for body in envelopes]) as http:
            snapshot = runner.HostedContainers.provider_snapshot(self.trial)
        self.assertEqual(list(snapshot["namespaces"]), [row["id"] for row in namespace_rows])
        self.assertEqual(snapshot["applications"], {row["id"]: row["name"] for row in applications})
        self.assertEqual(snapshot["registry"], {"foreign-image": frozenset({"release"})})
        self.assertTrue(http.call_args_list[2].args[0].endswith("/namespaces?page=2&per_page=100"))
        self.assertTrue(http.call_args_list[3].args[0].endswith("/containers/applications"))

    def test_unbounded_applications_read_accepts_raw_array(self):
        self.trial.provider_origin = "https://provider.example.test"
        self.trial.cfg = {"managementToken": "fixture"}
        apps = [{"id": "application", "name": "foreign-app"}]
        with patch.object(runner, "request_json", return_value=(200, apps)):
            self.assertEqual(self.trial.inventory(self.trial.account_root + "/containers/applications", cursor=True), apps)

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
            (True, {"next_page_token": "opaque", "page_token": ""}, {"page_token": "opaque"}, "page_token", "opaque"),
            (False, {"page": 1, "total_pages": 2, "total_count": 2, "per_page": 1},
             {"page": 2, "total_pages": 2}, "page", 2),
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

    def test_incomplete_metadata_is_rejected(self):
        row = [{"id": "1"}]
        cases = (
            (True, {"per_page": 1}, "no continuation"),
            (True, {"count": 0}, "count mismatch"),
            (True, {"total_count": 0}, "total_count mismatch"),
            (True, {"total_count": 2}, "ended before total_count"),
            (True, {"page_token": "unexpected"}, "cursor echo mismatch"),
            (False, None, "Missing paginated inventory metadata"),
            (False, {"page": 1, "total_pages": "1"}, "Invalid inventory total_pages"),
            (False, {"page": 2, "total_pages": 2}, "page number mismatch"),
            (False, {"page": 1, "total_pages": 1, "next_page_token": "cursor"}, "Mixed inventory"),
            (False, {"page": 1, "total_pages": 2, "total_count": 1, "per_page": 1}, "continuation is inconsistent"),
        )
        for cursor, metadata, message in cases:
            with self.subTest(metadata=metadata):
                self.trial.inventory_page = Mock(return_value=(row, metadata))
                with self.assertRaisesRegex(AssertionError, message):
                    self.trial.inventory("/inventory", cursor=cursor)

    def test_numbered_inventory_cannot_end_before_its_total(self):
        self.trial.inventory_page = Mock(side_effect=[
            ([{"id": "first"}], {"page": 1, "per_page": 1, "count": 1, "total_count": 2}),
            ([], {"page": 2, "per_page": 1, "count": 0, "total_count": 2}),
        ])
        with self.assertRaisesRegex(AssertionError, "continuation is inconsistent"):
            self.trial.inventory("/inventory")

    def test_numbered_inventory_can_finish_using_reported_page_size(self):
        rows = [{"id": str(index)} for index in range(3)]
        self.trial.inventory_page = Mock(side_effect=[
            (rows[:2], {"page": 1, "per_page": 2}),
            (rows[2:], {"page": 2, "per_page": 2}),
        ])
        self.assertEqual(self.trial.inventory("/inventory"), rows)

    def test_numbered_inventory_rejects_changing_total(self):
        self.trial.inventory_page = Mock(side_effect=[
            ([{"id": "1"}], {"page": 1, "per_page": 1, "total_count": 3}),
            ([{"id": "2"}], {"page": 2, "per_page": 1, "total_count": 2}),
        ])
        with self.assertRaisesRegex(AssertionError, "total_count mismatch"):
            self.trial.inventory("/inventory")

    def test_total_pages_without_count_requires_full_nonterminal_page(self):
        self.trial.inventory_page = Mock(return_value=([{"id": "1"}], {"page": 1, "per_page": 2, "total_pages": 2}))
        with self.assertRaisesRegex(AssertionError, "Short nonterminal numbered page"):
            self.trial.inventory("/inventory")
        self.trial.inventory_page.assert_called_once()

    def test_bare_array_is_rejected_outside_unbounded_applications(self):
        self.trial.provider_origin = "https://provider.example.test"
        self.trial.cfg = {"managementToken": "fixture"}
        with patch.object(runner, "request_json", return_value=(200, [{"id": "namespace"}])):
            with self.assertRaisesRegex(AssertionError, "Malformed inventory envelope"):
                self.trial.inventory(self.trial.account_root + "/workers/durable_objects/namespaces")

    def test_durable_object_health_accepts_additional_fields(self):
        for counts in ({"active": 0, "starting": 0, "assigned": 2}, {"active": 0, "starting": 0}):
            self.trial.verify_empty_application({"health": {"instances": counts}})
        for counts in ({"active": 0, "assigned": 0}, {"active": False, "starting": 0}, {"active": 1, "starting": 0}):
            with self.subTest(counts=counts), self.assertRaises(AssertionError):
                self.trial.verify_empty_application({"health": {"instances": counts}})

    def test_exact_inactive_placement_is_terminal(self):
        observed = {"id": "actor", "application_id": "owned", "status": {"state": "inactive"}}
        self.trial.verify_stopped_placement(observed, "actor", "owned")
        with self.assertRaises(AssertionError):
            self.trial.verify_stopped_placement(observed, "other", "owned")
        with self.assertRaises(AssertionError):
            self.trial.verify_stopped_placement({**observed, "status": {"state": "active"}}, "actor", "owned")

    def test_unattributed_recorded_namespace_is_a_teardown_leftover(self):
        self.plan["installation"] = "fixture"
        after = deepcopy(self.baseline)
        after["namespaces"]["owned-id"] = (None, None, "Launcher")
        self.trial.provider_snapshot.return_value = after
        self.trial.worker_receipts.return_value = [{"namespaces": {"Launcher": "owned-id"}}]
        self.trial.release_primary = Mock()
        self.trial.install = Mock(return_value=None)
        with patch.object(runner, "evidence") as emit, self.assertRaisesRegex(AssertionError, "still present"):
            self.trial.teardown(self.baseline, self.plan, None, None)
        closing, = [record for stage, record in (call.args for call in emit.call_args_list)
                    if stage == "closing-provider-inventory"]
        self.assertEqual(closing["trial_leftovers"]["namespaces"], ["owned-id"])
        self.assertFalse(closing["foreign_drift"])

    def test_script_list_requires_single_page_completeness(self):
        rows = [{"id": "worker"}]
        self.trial.inventory_page = Mock(return_value=(rows, {}))
        self.assertEqual(self.trial.scripts(), {"worker"})
        self.trial.inventory_page.assert_called_once_with("/client/v4/accounts/fixture/workers/scripts", {})
        self.trial.inventory_page.return_value = rows, {"total_count": 2}
        with self.assertRaisesRegex(AssertionError, "ended before total_count"):
            self.trial.scripts()
        self.trial.inventory_page.return_value = rows, {"next_page_token": "cursor"}
        with self.assertRaisesRegex(AssertionError, "Invalid inventory cursor continuation"):
            self.trial.scripts()

    def test_receipt_verification_checks_the_second_namespace_page(self):
        self.trial.scripts = Mock(return_value=set())
        self.trial.provider = Mock()
        self.trial.inventory_page = Mock(side_effect=[
            ([{"id": "foreign"}], {"page": 1, "total_pages": 2, "per_page": 1}),
            ([{"id": "trial-ns"}], {"page": 2, "total_pages": 2}),
        ])
        with self.assertRaises(AssertionError):
            self.trial.verify_receipts_absent([{"script": "trial-worker", "namespaces": {"class": "trial-ns"}}])
        self.assertEqual(self.trial.inventory_page.call_count, 2)
        self.trial.provider.assert_not_called()

    def test_foreign_drift_is_reported_without_marking_leftovers(self):
        after = deepcopy(self.baseline)
        after["scripts"]["another-foreign-script"] = "etag"
        after["scripts"]["trial-worker-2"] = "updated"
        del after["namespaces"]["ns1"]
        self.trial.provider_snapshot.return_value = after
        with unittest.mock.patch.object(runner, "evidence", wraps=runner.evidence) as emit:
            self.trial.closing_inventory(self.baseline, self.plan)
        stage, receipt = emit.call_args.args
        self.assertEqual(stage, "closing-provider-inventory")
        self.assertTrue(receipt["foreign_drift"])
        self.assertFalse(any(receipt["trial_leftovers"].values()))
        self.assertEqual(receipt["changes"]["scripts"]["changed"], [{"id": "trial-worker-2", "matches_plan": False}])
        self.assertEqual(receipt["changes"]["scripts"]["added"], [{"id": "another-foreign-script", "matches_plan": False}])
        self.assertEqual(receipt["changes"]["namespaces"]["removed"], [{"id": "ns1", "matches_plan": False}])

    def test_teardown_failure_still_records_inventory_and_preserves_journey_error(self):
        self.plan["installation"] = "fixture"
        original = ValueError("injected activation failure")
        after = deepcopy(self.baseline)
        after["applications"]["partial-app-id"] = self.plan["application"]
        self.trial.provider_snapshot.side_effect = [self.baseline, after]
        self.trial.enable = Mock(side_effect=original)
        self.trial.release_primary = Mock(side_effect=RuntimeError("injected release failure"))
        self.trial.install = Mock(side_effect=LookupError("injected installation read failure"))
        with unittest.mock.patch.object(runner, "evidence", wraps=runner.evidence) as emit:
            with self.assertRaises(ValueError) as failure:
                self.trial.run(self.plan)
        self.assertIs(failure.exception, original)
        self.assertEqual(self.trial.provider_snapshot.call_count, 2)
        receipts = [call.args for call in emit.call_args_list]
        self.assertIn(("trial-teardown-failed", {"step": "release-primary", "error_type": "RuntimeError"}), receipts)
        closing, = [record for stage, record in receipts if stage == "closing-provider-inventory"]
        self.assertEqual(closing["trial_leftovers"]["applications"], ["partial-app-id"])
        self.assertEqual(closing["changes"]["applications"]["added"], [{"id": "partial-app-id", "matches_plan": True}])
        self.assertFalse(closing["foreign_drift"])

    def test_disconnect_failure_remains_primary_when_snapshot_also_fails(self):
        self.plan["installation"] = "fixture"
        original = RuntimeError("injected disconnect failure")
        self.trial.release_primary = Mock()
        self.trial.install = Mock(return_value={"installation_id": "fixture"})
        self.trial.disconnect = Mock(side_effect=original)
        self.trial.provider_snapshot.side_effect = TimeoutError("injected snapshot failure")
        with unittest.mock.patch.object(runner, "evidence", wraps=runner.evidence) as emit:
            with self.assertRaises(RuntimeError) as failure:
                self.trial.teardown(self.baseline, self.plan, None, None)
        self.assertIs(failure.exception, original)
        self.assertEqual([call.args[1]["step"] for call in emit.call_args_list], ["disconnect", "closing-inventory"])

if __name__ == "__main__":
    unittest.main()
