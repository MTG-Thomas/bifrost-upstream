"""Pure and mocked control-plane tests for Phase 0. No cloud credentials."""

import json
import unittest
from unittest.mock import Mock

import httpx
from phase0 import Trial
from phase0_adapter import (
    QualificationError,
    active_entitlement,
    application_path,
    application_spec,
    observe_owned,
    pinned_registry_image,
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

    def test_lost_upload_response_observes_and_never_repeats_put(self):
        api = Mock()
        trial = Trial(Mock(), api, "bifrost-p0-0123456789ab", Mock())
        settings = {"bindings": [{"name": "PHASE0_OWNER", "type": "plain_text", "text": trial.owner}]}
        api.optional.side_effect = [None, settings, settings]
        api.upload.side_effect = httpx.ReadTimeout("fixture loses response after upload")
        trial.upload("runtime", "phase0-runtime.mjs", (), [])
        api.upload.assert_called_once()
        self.assertEqual(api.optional.call_count, 3)


if __name__ == "__main__":
    unittest.main()
