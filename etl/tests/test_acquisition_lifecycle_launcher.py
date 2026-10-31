import json
import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("scenario", "expected_rc", "expected_run", "expected_build", "expected_cleanup"),
    [
        ("foreign_container", 1, False, False, []),
        ("foreign_image", 1, False, False, []),
        ("build_failure", 37, False, True, []),
        ("shared_image_id", 0, True, True, ["stop", "rm -v", "image rm"]),
        ("run_collision", 125, True, True, ["image rm"]),
        ("success", 0, True, True, ["stop", "rm -v", "image rm"]),
    ],
)
def test_launcher_cleans_only_resources_owned_by_this_run(
    tmp_path: Path,
    scenario: str,
    expected_rc: int,
    expected_run: bool,
    expected_build: bool,
    expected_cleanup: list[str],
) -> None:
    root = Path(__file__).resolve().parents[2]
    script = root / "etl/tests/control/acquisition_lifecycle.sh"
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    log = tmp_path / "docker.jsonl"
    docker = fakebin / "docker"
    docker.write_text(
        '''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ["DOCKER_LOG"], "a") as f:
    f.write(json.dumps(args) + "\\n")
scenario = os.environ["DOCKER_SCENARIO"]
if args[:2] == ["container", "inspect"]:
    sys.exit(0 if scenario == "foreign_container" else 1)
if args[:2] == ["image", "inspect"]:
    if len(args) > 2 and args[2] == "-f":
        if scenario in {"shared_image_id", "run_collision", "success"}:
            image_id = (
                "sha256:preexisting-shared-image"
                if scenario == "shared_image_id"
                else "sha256:owned-image-id"
            )
            print(image_id)
            sys.exit(0)
        sys.exit(1)
    sys.exit(0 if scenario == "foreign_image" else 1)
if args[0] == "build":
    sys.exit(37 if scenario == "build_failure" else 0)
if args[0] == "run":
    if scenario in {"success", "shared_image_id"}:
        print("owned-container-id")
        sys.exit(0)
    sys.exit(125)
if args[0] == "logs" and args[1].startswith("bracc-f05-lifecycle-"):
    print("PostgreSQL init process complete; ready for start up.")
    sys.exit(0)
if args[0] == "exec" and any(arg.startswith("bracc-f05-lifecycle-") for arg in args):
    sys.exit(0)
if args[:2] == ["stop", "-t"] or args[:2] == ["rm", "-v"] or args[:2] == ["image", "rm"]:
    sys.exit(0)
sys.exit(0)
'''
    )
    docker.chmod(0o755)
    fake_python = fakebin / "python"
    fake_python.write_text(
        '''#!/usr/bin/env python3
import sys
if "-c" in sys.argv and "import psycopg" in sys.argv:
    sys.exit(0)
if any(arg.endswith("acquisition_lifecycle_postgres.py") for arg in sys.argv):
    sys.exit(0)
sys.exit(2)
'''
    )
    fake_python.chmod(0o755)
    env = os.environ | {
        "PATH": f"{fakebin}:{os.environ['PATH']}",
        "DOCKER_LOG": str(log),
        "DOCKER_SCENARIO": scenario,
        "BRACC_TEST_PYTHON": str(fake_python),
    }
    child_argv = ["bash", str(script)]
    child_cwd = str(root / "etl")
    result = subprocess.run(
        child_argv, cwd=child_cwd, env=env, text=True, capture_output=True
    )
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    (tmp_path / "execution-receipt.json").write_text(
        json.dumps(
            {
                "scenario": scenario,
                "argv": child_argv,
                "cwd": child_cwd,
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "docker_calls": calls,
            },
            indent=2,
        )
        + chr(10)
    )
    assert result.returncode == expected_rc, (result.stdout, result.stderr, calls)
    assert any(call[0] == "run" for call in calls) is expected_run
    assert any(call[0] == "build" for call in calls) is expected_build
    if expected_cleanup:
        cleanup_calls = [
            call
            for call in calls
            if call[0] == "stop" or call[:2] in (["rm", "-v"], ["image", "rm"])
        ]
        cleanup = [
            " ".join(call[:2]) if call[0] == "image" or call[:2] == ["rm", "-v"] else call[0]
            for call in cleanup_calls
        ]
        assert cleanup == expected_cleanup
    if expected_cleanup:
        owned_cleanup = [
            call
            for call in calls
            if call[0] in {"stop", "rm"} or call[:2] == ["image", "rm"]
        ]
        assert all(
            "owned-container-id" in call
            for call in owned_cleanup
            if call[0] in {"stop", "rm"}
        )
        if scenario == "shared_image_id":
            assert len([call for call in owned_cleanup if call[:2] == ["image", "rm"]]) == 1
            image_removals = [
                call for call in owned_cleanup if call[:2] == ["image", "rm"]
            ]
            assert image_removals[0][2].endswith(":test")
        if scenario == "build_failure":
            assert not any(call[:2] == ["image", "rm"] for call in calls)
    if scenario.startswith("foreign_"):
        assert not any(call[0] == "build" or call[0] == "run" for call in calls)
        assert not any(call[0] in {"stop", "rm"} or call[:2] == ["image", "rm"] for call in calls)
