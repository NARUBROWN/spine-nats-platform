"""NATS의 초기 채널 구성과 실제 검증 범위를 확인합니다."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/release"))

import configure_branches
import generated_core_proxy
import pin_channel
import release
import verify_public

ROOT = Path(__file__).resolve().parents[2]
POLICY = json.loads((ROOT / ".github/release-policy.json").read_text())


class NATSChannelTests(unittest.TestCase):
    def test_preview_only_repository_bootstraps_rc_and_main_without_moving_preview(self):
        class Repository:
            repository = "NARUBROWN/spine-nats-platform"
            def __init__(self):
                self.writes = []
            def api(self, path, **kwargs):
                if kwargs.get("method") == "POST":
                    self.writes.append(kwargs["data"])
                    return {}
                return {"object": {"sha": "a" * 40}} if path.endswith("/preview") else None
        github = Repository()
        configure_branches.bootstrap(github)
        self.assertEqual(github.writes, [])
        configure_branches.bootstrap(github, apply=True)
        self.assertEqual(github.writes, [{"ref": "refs/heads/rc", "sha": "a" * 40},
                                        {"ref": "refs/heads/main", "sha": "a" * 40}])

    def test_channel_pins_preserve_public_contract_baselines(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / ".github").mkdir()
            (root / ".github/release-policy.json").write_text(json.dumps(POLICY))
            for name in (".", "e2e", "examples/worker"):
                directory = root / name
                directory.mkdir(parents=True, exist_ok=True)
                module = POLICY["module"] + ("/" + name if name != "." else "")
                (directory / "go.mod").write_text(f"module {module}\n\ngo 1.26.0\n\nrequire {release.CORE} v0.7.0-preview.2\n")
            pin_channel.pin(root, "rc", apply=True)
            release.check_module(POLICY, "rc", root)
            self.assertIn("v0.7.0-preview.2", (root / "e2e/go.mod").read_text())
            (root / "e2e/go.mod").write_text("module example.com/child\n\ngo 1.26.0\n")
            with self.assertRaises(release.ReleaseError):
                release.check_module(POLICY, "rc", root)

    def test_generated_source_proxy_builds_the_selected_channel_core(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            version = "v0.7.0-preview.2"
            (root / "go.mod").write_text("module " + release.CORE + "\n\ngo 1.26.0\n")
            (root / "info.json").write_text(json.dumps({"Version": version, "Time": "2026-11-05T00:00:00Z"}))
            with zipfile.ZipFile(root / "core.zip", "w") as archive:
                for name, value in {"go.mod": (root / "go.mod").read_text(), "core.go": "package spine\nconst Value = 42\n"}.items():
                    archive.writestr(release.CORE + "@" + version + "/" + name, value)
            download = {"Version": version, "Info": str(root / "info.json"), "GoMod": str(root / "go.mod"), "Zip": str(root / "core.zip")}
            proxy = generated_core_proxy.write_proxy(download, "v0.7.0-rc.1", root / "proxy")
            consumer = root / "consumer"
            consumer.mkdir()
            (consumer / "go.mod").write_text("module example.com/channel\n\ngo 1.26.0\n\nrequire " + release.CORE + " v0.7.0-rc.1\n")
            (consumer / "main.go").write_text('package main\nimport "' + release.CORE + '"\nfunc main() { if spine.Value != 42 { panic("wrong source") } }\n')
            env = dict(os.environ, GOWORK="off", GOPROXY=proxy, GOSUMDB="off", GOMODCACHE=str(root / "modules"))
            subprocess.run(["go", "mod", "tidy"], cwd=consumer, env=env, check=True, capture_output=True)
            resolved = subprocess.check_output(["go", "list", "-m", "-json", release.CORE + "@preview"], cwd=consumer, env=env, text=True)
            self.assertEqual(json.loads(resolved)["Version"], "v0.7.0-rc.1")
            subprocess.run(["go", "run", "."], cwd=consumer, env=env, check=True, capture_output=True)

    def test_public_proxy_rejects_a_different_core_version(self):
        completed = subprocess.CompletedProcess([], 0, json.dumps({"Version": "v0.7.0-preview.1"}), "")
        baseline = release.CORE + " v0.7.0-preview.2 v0.7.0-preview.12"
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(generated_core_proxy.subprocess, "check_output", return_value=baseline), \
                patch.object(generated_core_proxy.subprocess, "run", return_value=completed):
            with self.assertRaises(release.ReleaseError):
                generated_core_proxy.prepare(POLICY, "rc", Path(temporary), public=True)

    def test_public_nats_verification_requires_tls_and_real_brokers(self):
        plan = {"module": POLICY["module"]}
        with tempfile.TemporaryDirectory() as temporary:
            for env in ({}, {"SPINE_TEST_NATS_URL": "nats://127.0.0.1:4222"}):
                with self.subTest(env=env), self.assertRaises(release.ReleaseError):
                    verify_public.verify(plan, ROOT, Path(temporary), env, "preview", "platform")

    def test_public_verification_ignores_packages_without_tests_but_rejects_test_skip(self):
        passed = [{"Action": "skip", "Package": "example.com/helper"},
                  {"Action": "run", "Test": "TestLive"}, {"Action": "pass", "Test": "TestLive"}]
        for events, rejects in ((passed, False), (passed + [{"Action": "skip", "Test": "TestTLS"}], True)):
            completed = subprocess.CompletedProcess([], 0, "\n".join(map(json.dumps, events)), "")
            with patch.object(verify_public.subprocess, "run", return_value=completed):
                if rejects:
                    with self.assertRaises(release.ReleaseError):
                        verify_public.go_tests([], ROOT, {})
                else:
                    verify_public.go_tests([], ROOT, {})


if __name__ == "__main__":
    unittest.main()
