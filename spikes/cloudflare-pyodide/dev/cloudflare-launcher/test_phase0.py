"""Pure and mocked control-plane tests for Phase 0. No cloud credentials."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
from phase0 import Cf, CfCommandError, ContainerTrial, Trial, registry_refused
from phase0_adapter import (
    Cloudflare,
    QualificationError,
    RegistryCopy,
    ResourceExists,
    active_entitlement,
    application_path,
    application_spec,
    matches_application,
    observe_owned,
    pinned_registry_image,
    subprocess_environment,
    worker_parts,
)


class ProvisioningTest(unittest.TestCase):
    def test_module_and_secret_metadata_use_separate_multipart_parts(self):
        metadata = {
            "main_module": "entry.mjs",
            "bindings": [{"type": "secret_text", "name": "TRIAL", "text": "fixture"}],
        }
        parts = worker_parts({"entry.mjs": b"export default {};"}, metadata)
        self.assertEqual(json.loads(parts["metadata"][1]), metadata)
        self.assertEqual(parts["entry.mjs"], ("entry.mjs", b"export default {};", "application/javascript+module"))
        with self.assertRaises(QualificationError):
            worker_parts({}, metadata)

    def test_namespace_bound_application_selects_durable_object_scheduling(self):
        namespace = "a" * 32
        self.assertEqual(application_spec("trial", namespace), {
            "name": "trial",
            "scheduling_policy": "durable_object",
            "durable_objects": {"namespace_id": namespace},
        })
        with self.assertRaises(QualificationError):
            application_spec("trial", "different-resource-type")

    def test_application_lookup_preserves_provider_identifier(self):
        opaque = "a" * 32
        self.assertEqual(application_path(opaque), f"containers/applications/{opaque}")
        with self.assertRaises(QualificationError):
            application_path("../other")

    def test_registry_reference_is_fenced_to_the_account_trial_and_digest(self):
        account = "a" * 32
        name = "bifrost-p0-0123456789ab"
        digest = "sha256:" + "b" * 64
        self.assertEqual(pinned_registry_image(account, name, digest),
                         f"registry.cloudflare.com/{account}/{name}@{digest}")
        for values in (("other", name, digest), (account, "other", digest), (account, name, "latest")):
            with self.subTest(values=values), self.assertRaises(QualificationError):
                pinned_registry_image(*values)

    def test_deleted_or_disabled_entitlements_do_not_enable_a_scope(self):
        row = {"id": "containers.enabled", "deleted_date": "", "allocation": {"type": "bool", "value": True}}
        self.assertTrue(active_entitlement([row], "containers.enabled"))
        self.assertFalse(active_entitlement([{**row, "deleted_date": "2026-10-10"}], "containers.enabled"))
        self.assertFalse(active_entitlement([{**row, "allocation": {"type": "bool", "value": False}}],
                                            "containers.enabled"))

    def test_existing_worker_with_foreign_marker_is_refused(self):
        settings = {"bindings": [{"name": "PHASE0_OWNER", "type": "plain_text", "text": "another-trial"}]}
        with self.assertRaisesRegex(QualificationError, "ownership conflict"):
            observe_owned(settings, "current-trial")

    def upload_fixture(self):
        api = Cloudflare("a" * 32, "fixture")
        self.addCleanup(api.http.close)
        api.request = Mock(side_effect=httpx.ReadTimeout("fixture loses response after upload"))
        api.optional = Mock()
        trial = Trial(Mock(), api, "bifrost-p0-0123456789ab", Mock())
        settings = {"bindings": [{"name": "PHASE0_OWNER", "type": "plain_text", "text": trial.owner}]}
        return trial, api, settings

    def test_lost_upload_response_observes_and_never_repeats_put(self):
        trial, api, settings = self.upload_fixture()
        api.optional.side_effect = [None, settings]
        trial.upload("runtime", "phase0-runtime.mjs", (), [])
        api.request.assert_called_once()
        self.assertEqual(api.upload_attempts, 1)
        self.assertEqual(api.optional.call_count, 2)

    def test_timeout_without_commit_stops_without_a_second_put(self):
        trial, api, _ = self.upload_fixture()
        api.optional.return_value = None
        with (patch("phase0.time.monotonic", side_effect=[0, 0, 31]),
              self.assertRaisesRegex(QualificationError, "readback did not confirm")):
            trial.upload("runtime", "phase0-runtime.mjs", (), [])
        self.assertEqual(api.upload_attempts, 1)
        api.inventory = Mock(return_value=[])
        with self.assertRaisesRegex(QualificationError, "Worker teardown incomplete"):
            trial.cleanup_workers()
        api.request.assert_called_once()

    def test_foreign_marker_after_timeout_blocks_adoption(self):
        trial, api, settings = self.upload_fixture()
        settings["bindings"][0]["text"] = "foreign"
        api.optional.side_effect = [None, settings]
        with self.assertRaisesRegex(QualificationError, "ownership conflict"):
            trial.upload("runtime", "phase0-runtime.mjs", (), [])
        self.assertEqual(api.upload_attempts, 1)

    def test_occupied_worker_is_refused_by_upload_without_a_put(self):
        trial, api, settings = self.upload_fixture()
        api.optional.return_value = settings
        with self.assertRaises(ResourceExists):
            trial.upload("runtime", "phase0-runtime.mjs", (), [])
        self.assertEqual(api.upload_attempts, 0)
        api.request.assert_not_called()

    def test_inventory_follows_page_numbers_and_validates_total(self):
        _, api, _ = self.upload_fixture()
        api.envelope = Mock(side_effect=[
            {"result": [{"id": "first"}], "result_info": {"page": 1, "count": 1, "total_count": 2}},
            {"result": [{"id": "second"}], "result_info": {"page": 2, "count": 1, "total_count": 2}},
        ])
        self.assertEqual([row["id"] for row in api.inventory("namespaces")], ["first", "second"])
        self.assertEqual([call.kwargs["params"]["page"] for call in api.envelope.call_args_list], [1, 2])

    def test_inventory_follows_cursor_and_rejects_incomplete_total(self):
        _, api, _ = self.upload_fixture()
        api.envelope = Mock(side_effect=[
            {"result": [{"id": "first"}], "result_info": {"next_page_token": "next", "total_count": 3}},
            {"result": [{"id": "second"}], "result_info": {"total_count": 3}},
        ])
        with self.assertRaisesRegex(QualificationError, "total did not reconcile"):
            api.inventory("applications", cursor=True, name="trial")
        self.assertEqual(api.envelope.call_args.kwargs["params"]["page_token"], "next")

    def test_namespace_readback_runs_after_worker_delete_failure(self):
        trial, api, settings = self.upload_fixture()
        trial.worker_intents = [trial.prefix + "-runtime"]
        api.optional.return_value = settings
        api.inventory = Mock(side_effect=QualificationError("namespace unavailable"))
        with self.assertRaisesRegex(QualificationError, "script runtime.*namespace readback"):
            trial.cleanup_workers()
        api.inventory.assert_called_once()

    def test_container_teardown_checks_all_stages_after_application_failure(self):
        trial, _, _ = self.upload_fixture()
        trial.cleanup_workers = Mock(side_effect=QualificationError("namespace unavailable"))
        container = ContainerTrial(trial, Mock())
        self.addCleanup(container.caller.close)
        container.instance_may_be_live = True
        container.copy_intent = True
        container.owned_application = Mock(side_effect=QualificationError("application unavailable"))
        container.app_intent = application_spec(container.name, "b" * 32)
        container.images = Mock(return_value=[{}])
        with self.assertRaisesRegex(QualificationError, "application:.*Workers and namespaces:.*registry:"):
            container.cleanup()
        trial.cleanup_workers.assert_called_once_with(allow_delete=False)
        container.images.assert_called_once()
        container.crane.credentials.assert_not_called()

    def test_failed_teardown_retains_token_and_success_deletes_exact_identity(self):
        account = "a" * 32
        token_id = "b" * 32
        prefix = "bifrost-p0-0123456789ab"
        with tempfile.TemporaryDirectory() as directory:
            cf = Cf.__new__(Cf)
            cf.directory = Path(directory)
            group_rows = [{"name": key, "id": key} for key in ("Account Settings Read", "Workers Scripts Write")]
            created = {"id": token_id, "value": "fixture"}
            cf.json = Mock(side_effect=[group_rows, created])
            with self.assertRaises(QualificationError), cf.scoped_token(account, prefix, False, Mock()):
                raise QualificationError("cleanup failed")
            self.assertEqual(cf.json.call_count, 2)
            cf.json = Mock(side_effect=[
                group_rows, created, {"id": token_id, "name": prefix + "-auth"}, None,
                CfCommandError("accounts tokens get", 404),
            ])
            with cf.scoped_token(account, prefix, False, Mock()) as (_, confirm):
                confirm()
            self.assertEqual(cf.json.call_args_list[-2].args,
                             ("accounts", "tokens", "delete", token_id, "--force"))

    def test_child_environment_excludes_platform_secrets(self):
        with patch.dict(os.environ, {"PATH": "bin", "BIFROST_SECRET_KEY": "secret", "DATABASE_URL": "secret",
                                     "CLOUDFLARE_API_TOKEN": "secret"}, clear=True):
            self.assertEqual(subprocess_environment(), {"PATH": "bin"})

    def test_registry_credentials_are_removed_after_failed_copy(self):
        _, api, _ = self.upload_fixture()
        api.request = Mock(return_value={
            "account_id": api.account, "registry_host": "registry.cloudflare.com",
            "username": "fixture", "password": "fixture",
        })
        with tempfile.TemporaryDirectory() as directory, patch.object(RegistryCopy, "run", return_value="v0.20.6"):
            crane = RegistryCopy(Path(__file__), Path(directory))
            with self.assertRaises(ValueError), crane.credentials(api, push=True):
                config = Path(crane.env["DOCKER_CONFIG"])
                self.assertTrue((config / "config.json").is_file())
                raise ValueError("copy failed")
            self.assertFalse(config.exists())
            with crane.credentials(api):
                self.assertEqual(api.request.call_args.kwargs["json"]["permissions"], ["pull"])

    def test_application_comparison_allows_other_namespace_metadata(self):
        spec = application_spec("trial", "a" * 32)
        row = {**spec, "durable_objects": {**spec["durable_objects"], "other": "value"}}
        self.assertTrue(matches_application(row, spec))
        self.assertFalse(matches_application({**row, "durable_objects": {"namespace_id": "b" * 32}}, spec))

    def test_empty_conflict_is_not_evidence_of_registry_refusal(self):
        self.assertFalse(registry_refused(httpx.Response(409)))
        self.assertTrue(registry_refused(httpx.Response(409, json={"ok": False})))


if __name__ == "__main__":
    unittest.main()
