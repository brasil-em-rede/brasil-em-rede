from __future__ import annotations

import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "etl/tests/control/evidence_journal.sh"


def _run_with_inert_docker(
    tmp_path: Path, mode: str
) -> tuple[subprocess.CompletedProcess[bytes], dict[str, object]]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state_path = tmp_path / "docker-state.json"
    state: dict[str, object] = {
        "mode": mode,
        "calls": [],
        "image": (
            None
            if mode == "build_failure_foreign_image"
            else "pre-existing-image-sentinel"
        ),
        "container": (
            "pre-existing-container-sentinel" if mode == "preexisting_container" else None
        ),
        "image_inspections": 0,
    }
    state_path.write_text(json.dumps(state))
    docker = fake_bin / "docker"
    docker.write_text(
        r"""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
path = Path(os.environ['U05_FAKE_DOCKER_STATE'])
state = json.loads(path.read_text())
args = sys.argv[1:]
mode = state['mode']
code, out, err = 0, '', ''
if args[:2] == ['container', 'inspect']:
    if mode == 'preexisting_container':
        out = json.dumps([{'Id': state['container']}]) + '\n'
    elif mode == 'error_container':
        code = 125
        err = 'Cannot connect to the Docker daemon: synthetic inspect failure\n'
    else:
        code = 1
        err = 'Error response from daemon: No such container: ' + args[-1] + '\n'
elif args[:2] == ['image', 'inspect']:
    state['image_inspections'] += 1
    if mode == 'preexisting_image':
        out = json.dumps([{'Id': state['image']}]) + '\n'
    elif mode.startswith('error_') and state['image_inspections'] == 1:
        code = 125
        err = 'Cannot connect to the Docker daemon: synthetic inspect failure\n'
    elif state['image']:
        out = json.dumps([{'Id': state['image']}]) + '\n'
    else:
        code = 1
        err = 'Error response from daemon: No such image: ' + args[-1] + '\n'
elif args[:1] == ['build']:
    if mode == 'build_failure_foreign_image':
        state['image'] = 'foreign-image-sentinel'
    code = 86
    err = 'synthetic build failure before image acquisition\n'
elif args[:2] == ['image', 'rm']:
    state['image'] = None
elif args[:1] == ['run']:
    code = 87
    err = 'unexpected container launch\n'
else:
    code = 98
    err = 'unexpected inert Docker argv\n'
state['calls'].append({'argv': args, 'returncode': code})
path.write_text(json.dumps(state))
sys.stdout.write(out)
sys.stderr.write(err)
raise SystemExit(code)
"""
    )
    docker.chmod(0o700)
    selected_python = fake_bin / "selected-python"
    selected_python.write_text("#!/bin/sh\n[ \"$1\" = \"-c\" ] && exit 0\nexit 97\n")
    selected_python.chmod(0o700)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    env = os.environ.copy()
    for key in (
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONUSERBASE",
        "BRACC_EVIDENCE_TMPDIR",
        "F09_NEO4J_URI",
        "F09_NEO4J_PASSWORD",
        "BRACC_TEST_NEO4J_URI",
        "IDENTITY_TEST_NEO4J_URI",
    ):
        env.pop(key, None)
    env.update(
        PATH=f"{fake_bin}{os.pathsep}{env['PATH']}",
        TMPDIR=str(scratch),
        BRACC_TEST_PYTHON=str(selected_python),
        BRACC_EVIDENCE_ATTEMPT_ID=f"u05-quality-{uuid.uuid4().hex}",
        U05_FAKE_DOCKER_STATE=str(state_path),
    )
    result = subprocess.run(
        ["bash", "etl/tests/control/evidence_journal.sh"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        timeout=20,
    )
    state_after = json.loads(state_path.read_text())
    (tmp_path / "runner.stdout.bin").write_bytes(result.stdout)
    (tmp_path / "runner.stderr.bin").write_bytes(result.stderr)
    (tmp_path / "runner.receipt.json").write_text(
        json.dumps(
            {
                "argv": ["bash", "etl/tests/control/evidence_journal.sh"],
                "cwd": str(ROOT),
                "returncode": result.returncode,
                "selected_python": str(selected_python),
            },
            indent=2,
        )
        + "\n"
    )
    (tmp_path / "docker-state-after.json").write_text(
        json.dumps(state_after, indent=2) + "\n"
    )
    return result, state_after


def test_preexisting_container_is_refused_before_build_or_cleanup(tmp_path: Path) -> None:
    result, state = _run_with_inert_docker(tmp_path, "preexisting_container")
    calls = state["calls"]
    assert result.returncode == 1
    assert b"Refusing pre-existing Docker resource name/tag" in result.stderr
    assert [call["argv"][:2] for call in calls] == [["container", "inspect"]]
    assert state["container"] == "pre-existing-container-sentinel"
    assert state["image"] == "pre-existing-image-sentinel"


def test_build_failure_never_removes_a_tag_not_acquired_by_this_run(tmp_path: Path) -> None:
    result, state = _run_with_inert_docker(tmp_path, "build_failure_foreign_image")
    calls = state["calls"]
    assert result.returncode == 86
    assert any(call["argv"][:1] == ["build"] for call in calls)
    assert not any(call["argv"][:2] == ["image", "rm"] for call in calls), (
        "failed build did not acquire the image tag"
    )
    assert state["image"] == "foreign-image-sentinel"


def test_preexisting_image_is_refused_before_build_or_cleanup(tmp_path: Path) -> None:
    result, state = _run_with_inert_docker(tmp_path, "preexisting_image")
    calls = state["calls"]
    assert result.returncode == 1
    assert b"Refusing pre-existing Docker resource name/tag" in result.stderr
    assert [call["argv"][:2] for call in calls] == [
        ["container", "inspect"],
        ["image", "inspect"],
    ]
    assert state["image"] == "pre-existing-image-sentinel"


@pytest.mark.parametrize("error_target", ["container", "image"])
def test_inspect_errors_fail_closed_without_build_or_removing_unacquired_image(
    tmp_path: Path, error_target: str
) -> None:
    result, state = _run_with_inert_docker(tmp_path, f"error_{error_target}")
    calls = state["calls"]
    assert not any(call["argv"][:1] == ["build"] for call in calls), (
        "build must not run after Docker inspection failed"
    )
    assert not any(call["argv"][:2] == ["image", "rm"] for call in calls), (
        "an image not acquired by this run must not be removed"
    )
    assert result.returncode != 0
    assert state["image"] == "pre-existing-image-sentinel"


def _run_setup_fault(
    tmp_path: Path, mode: str
) -> tuple[subprocess.CompletedProcess[bytes], list[str], list[Path], str]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    docker_log = tmp_path / "docker.log"
    selected_log = tmp_path / "selected-python.log"

    docker = fake_bin / "docker"
    docker.write_text(
        r"""#!/bin/sh
printf '%s\n' "$*" >> "$U05_DOCKER_LOG"
if [ "$1" = container ] && [ "$2" = inspect ]; then
  printf 'Error response from daemon: No such container: %s\n' "$3" >&2
  exit 1
fi
if [ "$1" = image ] && [ "$2" = inspect ]; then
  printf 'Error response from daemon: No such image: %s\n' "$3" >&2
  exit 1
fi
printf 'unexpected inert Docker argv: %s\n' "$*" >&2
exit 99
"""
    )
    docker.chmod(0o700)

    selected_python = fake_bin / "selected-python"
    selected_python.write_text(
        r"""#!/bin/sh
printf '%s\n' "$*" >> "$U05_SELECTED_PYTHON_LOG"
if [ "$1" = -c ] && printf '%s' "$2" | grep -q psycopg; then exit 0; fi
if [ "$1" = -c ] && printf '%s' "$2" | grep -q socket; then
  [ "$U05_FAULT_MODE" = port_failure ] && exit 73
  printf '54321\n'
  exit 0
fi
exit 97
"""
    )
    selected_python.chmod(0o700)

    generic_python = fake_bin / "python"
    generic_python.write_text(
        r"""#!/bin/sh
[ "$U05_FAULT_MODE" = port_failure ] && exit 73
printf '54321\n'
"""
    )
    generic_python.chmod(0o700)

    fake_chmod = fake_bin / "chmod"
    fake_chmod.write_text(
        r"""#!/bin/sh
[ "$U05_FAULT_MODE" = chmod_failure ] && exit 73
exec /bin/chmod "$@"
"""
    )
    fake_chmod.chmod(0o700)

    env = os.environ.copy()
    for key in (
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONUSERBASE",
        "BRACC_EVIDENCE_TMPDIR",
        "F09_NEO4J_URI",
        "F09_NEO4J_PASSWORD",
        "BRACC_TEST_NEO4J_URI",
        "IDENTITY_TEST_NEO4J_URI",
    ):
        env.pop(key, None)
    env.update(
        PATH=f"{fake_bin}{os.pathsep}{env['PATH']}",
        TMPDIR=str(scratch),
        BRACC_TEST_PYTHON=str(selected_python),
        BRACC_EVIDENCE_ATTEMPT_ID=f"u05-setup-{uuid.uuid4().hex}",
        U05_DOCKER_LOG=str(docker_log),
        U05_FAULT_MODE=mode,
        U05_SELECTED_PYTHON_LOG=str(selected_log),
    )
    result = subprocess.run(
        ["bash", "etl/tests/control/evidence_journal.sh"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        timeout=20,
    )
    (tmp_path / "runner.stdout.bin").write_bytes(result.stdout)
    (tmp_path / "runner.stderr.bin").write_bytes(result.stderr)
    docker_calls = docker_log.read_text().splitlines() if docker_log.exists() else []
    selected_calls = selected_log.read_text() if selected_log.exists() else ""
    return result, docker_calls, list(scratch.iterdir()), selected_calls


@pytest.mark.parametrize("fault", ["port_failure", "chmod_failure"])
def test_setup_failure_uses_selected_python_and_removes_owned_scratch(
    tmp_path: Path, fault: str
) -> None:
    result, docker_calls, remaining, selected_calls = _run_setup_fault(tmp_path, fault)
    assert result.returncode == 73, (selected_calls, docker_calls, remaining)
    assert not any(call.startswith("build ") for call in docker_calls), docker_calls
    assert remaining == [], remaining
    if fault == "port_failure":
        assert "socket" in selected_calls, (selected_calls, docker_calls, remaining)


def _run_cleanup_fault(
    tmp_path: Path, mode: str
) -> tuple[subprocess.CompletedProcess[bytes], dict[str, object]]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state_path = tmp_path / "docker-state.json"
    state: dict[str, object] = {
        "mode": mode,
        "calls": [],
        "image": None,
        "container": None,
        "volume": None,
        "image_inspections": 0,
        "container_formatted_inspections": 0,
    }
    state_path.write_text(json.dumps(state))
    docker = fake_bin / "docker"
    docker.write_text(
        r"""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
path = Path(os.environ['U05_FAKE_DOCKER_STATE'])
state = json.loads(path.read_text())
args = sys.argv[1:]
mode = state['mode']
code, out, err, action = 0, '', '', ''
def missing(kind, target):
    return 'Error response from daemon: No such ' + kind + ': ' + target + '\n'
def formatted_container():
    item = state['container']
    mounts = [
        {
            'Type': 'volume',
            'Name': state['volume'],
            'Destination': '/var/lib/postgresql/data',
        }
    ]
    owner = item['owner']
    inspections = state['container_formatted_inspections']
    if mode == 'container_readback_owner_mismatch' and inspections == 1:
        owner = 'foreign-owner'
    elif mode == 'cleanup_container_owner_changed' and inspections > 1:
        owner = 'foreign-owner'
        state['container']['owner'] = owner
    return item['id'] + '|/' + item['name'] + '|' + owner + '|' + json.dumps(mounts) + '\n'

if args[:2] == ['container', 'inspect']:
    target = args[-1]
    if state['container'] is None:
        code, err = 1, missing('container', target)
    elif '--format' in args:
        state['container_formatted_inspections'] += 1
        cleanup_inspection_error = mode in {
            'cleanup_container_inspect_error',
            'payload_failure_cleanup_error',
        }
        if cleanup_inspection_error and state['container_formatted_inspections'] > 1:
            code, err = 125, 'Cannot connect to the Docker daemon: synthetic cleanup failure\n'
        else:
            out = formatted_container()
    else:
        out = json.dumps([{'Id': state['container']['id']}]) + '\n'
elif args[:2] == ['image', 'inspect']:
    state['image_inspections'] += 1
    target = args[-1]
    if state['image'] is None:
        code, err = 1, missing('image', target)
    elif (
        mode == 'image_readback_error'
        and '--format' in args
        and state['image_inspections'] == 2
    ):
        code = 125
        err = 'Cannot connect to the Docker daemon: synthetic readback failure\n'
    elif (
        mode == 'cleanup_image_inspect_error'
        and (
            ('--format' in args and state['image_inspections'] > 2)
            or ('--format' not in args and state['image_inspections'] > 1)
        )
    ):
        code = 125
        err = 'Cannot connect to the Docker daemon: synthetic cleanup failure\n'
    elif '--format' in args:
        item = state['image']
        owner = item['owner']
        if mode == 'cleanup_image_owner_changed' and state['image_inspections'] > 2:
            owner = 'foreign-owner'
            state['image']['owner'] = owner
        out = item['id'] + '|' + json.dumps([item['tag']]) + '|' + owner + '\n'
    else:
        out = json.dumps([{'Id': state['image']['id']}]) + '\n'

elif args[:2] == ['volume', 'inspect']:
    if state['volume'] is None:
        code, err = 1, missing('volume', args[-1])
    else:
        out = json.dumps([{'Name': state['volume']}]) + '\n'
elif args[:1] == ['build']:
    tag = args[args.index('-t') + 1]
    owner = next(
        (value.split('=', 1)[1] for value in args if value.startswith('bracc.owner=')),
        None,
    )
    state['image'] = {'id': 'sha256:' + 'a' * 64, 'tag': tag, 'owner': owner}
    action = 'acquired_image'
elif args[:1] == ['run']:
    name = args[args.index('--name') + 1]
    owner = next(value.split('=', 1)[1] for value in args if value.startswith('bracc.owner='))
    state['container'] = {'id': 'c' * 64, 'name': name, 'owner': owner}
    state['volume'] = '0' * 64
    out = state['container']['id'] + '\n'
    action = 'acquired_container_and_volume'
elif args[:1] == ['inspect'] and '--format' in args:
    mounts = [
        {
            'Type': 'volume',
            'Name': state['volume'],
            'Destination': '/var/lib/postgresql/data',
        }
    ]
    out = 'MOUNTS ' + json.dumps(mounts) + '\n'
elif args[:1] == ['inspect']:
    if state['container'] is None:
        code, err = 1, missing('container', args[-1])
    else:
        if mode in {'cleanup_container_inspect_error', 'payload_failure_cleanup_error'}:
            code = 125
            err = 'Cannot connect to the Docker daemon: synthetic cleanup failure'
        elif mode == 'cleanup_container_owner_changed':
            state['container']['owner'] = 'foreign-owner'
        out = json.dumps([{'Id': state['container']['id']}]) + '\n'

elif args[:1] == ['logs']:
    out = 'PostgreSQL init process complete; ready for start up.\n'
elif args[:1] == ['exec']:
    if '-i' in args:
        sys.stdin.buffer.read()
elif args[:1] == ['stop']:
    action = 'stopped_container'
elif args[:1] == ['rm']:
    if mode == 'cleanup_container_remove_error':
        code, err = 31, 'synthetic container remove failure\n'
    elif mode != 'cleanup_container_unconfirmed':
        state['container'] = None
        state['volume'] = None
        action = 'removed_container_and_volume'
elif args[:2] == ['image', 'rm']:
    if mode == 'cleanup_image_remove_error':
        code, err = 32, 'synthetic image remove failure\n'
    else:
        state['image'] = None
        action = 'removed_image_tag'
else:
    code, err = 98, 'unsupported inert Docker argv\n'
call = {
    'argv': args,
    'returncode': code,
    'stdout': out,
    'stderr': err,
    'action': action,
}
state['calls'].append(call)
path.write_text(json.dumps(state, indent=2) + '\n')
sys.stdout.write(out)
sys.stderr.write(err)
raise SystemExit(code)
"""
    )
    docker.chmod(0o700)
    selected_python = fake_bin / "selected-python"
    selected_python.write_text(
        r"""#!/bin/sh
if [ "$1" = -c ] && printf '%s' "$2" | grep -q psycopg; then exit 0; fi
if [ "$1" = -c ] && printf '%s' "$2" | grep -q socket; then printf '49152\n'; exit 0; fi
if [ "$1" = -c ] && printf '%s' "$2" | grep -q json.loads; then printf '%064d\n' 0; exit 0; fi
case "$*" in
  *evidence_journal_postgres.py*)
    if [ "$U05_FAULT_MODE" = payload_failure_cleanup_error ]; then
      printf 'synthetic payload failure\n' >&2
      exit 37
    fi
    exit 0
    ;;
esac
exit 97
"""
    )
    selected_python.chmod(0o700)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    env = os.environ.copy()
    for key in (
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONUSERBASE",
        "BRACC_EVIDENCE_TMPDIR",
        "F09_NEO4J_URI",
        "F09_NEO4J_PASSWORD",
        "BRACC_TEST_NEO4J_URI",
        "IDENTITY_TEST_NEO4J_URI",
    ):
        env.pop(key, None)
    env.update(
        PATH=f"{fake_bin}{os.pathsep}{env['PATH']}",
        TMPDIR=str(scratch),
        BRACC_TEST_PYTHON=str(selected_python),
        BRACC_EVIDENCE_ATTEMPT_ID=f"u05-cleanup-{uuid.uuid4().hex}",
        U05_FAKE_DOCKER_STATE=str(state_path),
        U05_FAULT_MODE=mode,
    )
    result = subprocess.run(
        ["bash", "etl/tests/control/evidence_journal.sh"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        timeout=30,
    )
    state_after = json.loads(state_path.read_text())
    (tmp_path / "runner.stdout.bin").write_bytes(result.stdout)
    (tmp_path / "runner.stderr.bin").write_bytes(result.stderr)
    (tmp_path / "docker-state-after.json").write_text(json.dumps(state_after, indent=2) + "\n")
    return result, state_after


@pytest.mark.parametrize(
    ("fault", "expected_status", "diagnostic"),
    [
        ("cleanup_container_inspect_error", 1, b"Cleanup could not confirm owned container"),
        ("cleanup_container_remove_error", 1, b"Cleanup could not remove owned container"),
        ("cleanup_image_inspect_error", 1, b"Cleanup could not confirm owned image tag"),
        ("cleanup_image_remove_error", 1, b"Cleanup could not remove owned image tag"),
        ("cleanup_container_unconfirmed", 1, b"Container remains after cleanup"),
        ("payload_failure_cleanup_error", 37, b"Cleanup could not confirm owned container"),
    ],
)
def test_cleanup_fault_never_reports_success_and_preserves_primary_status(
    tmp_path: Path, fault: str, expected_status: int, diagnostic: bytes
) -> None:
    result, state = _run_cleanup_fault(tmp_path, fault)
    assert result.returncode == expected_status, state
    assert diagnostic in result.stderr, result.stderr
    if fault in {
        "cleanup_container_inspect_error",
        "cleanup_container_remove_error",
        "cleanup_container_unconfirmed",
        "payload_failure_cleanup_error",
    }:
        assert state["container"] is not None and state["volume"] is not None, state
    else:
        assert state["container"] is None and state["volume"] is None, state
    if fault in {"cleanup_image_inspect_error", "cleanup_image_remove_error"}:
        assert state["image"] is not None, state
    else:
        assert state["image"] is None, state
    if fault == "payload_failure_cleanup_error":
        assert b"synthetic payload failure" in result.stderr


def _docker_call_argvs(state: dict[str, object]) -> list[list[str]]:
    calls = cast("list[dict[str, Any]]", state["calls"])
    return [cast("list[str]", call["argv"]) for call in calls]


def test_image_readback_error_does_not_acquire_or_remove_unverified_tag(
    tmp_path: Path,
) -> None:
    result, state = _run_cleanup_fault(tmp_path, "image_readback_error")
    calls = _docker_call_argvs(state)
    assert result.returncode == 1, state
    assert b"Built image identity/tag readback failed" in result.stderr
    assert not any(argv[:1] == ["run"] for argv in calls), calls
    assert not any(argv[:2] == ["image", "rm"] for argv in calls), calls
    assert state["container"] is None
    assert state["image"] is not None
    assert list((tmp_path / "scratch").iterdir()) == []


def test_container_identity_mismatch_is_not_acquired_or_removed(
    tmp_path: Path,
) -> None:
    result, state = _run_cleanup_fault(tmp_path, "container_readback_owner_mismatch")
    calls = _docker_call_argvs(state)
    assert result.returncode == 1, state
    assert b"New container identity/owner readback did not match" in result.stderr
    assert any(argv[:1] == ["run"] for argv in calls), calls
    assert not any(argv[0] in {"stop", "rm"} for argv in calls), calls
    assert state["container"] is not None and state["volume"] is not None
    assert state["image"] is None
    assert list((tmp_path / "scratch").iterdir()) == []


def test_cleanup_refuses_container_after_owner_readback_changes(tmp_path: Path) -> None:
    result, state = _run_cleanup_fault(tmp_path, "cleanup_container_owner_changed")
    calls = _docker_call_argvs(state)
    assert result.returncode == 1, state
    assert b"Cleanup refused container whose identity or owner changed" in result.stderr
    assert not any(argv[0] in {"stop", "rm"} for argv in calls), calls
    assert state["container"] is not None and state["volume"] is not None
    assert state["image"] is None
    assert list((tmp_path / "scratch").iterdir()) == []


def test_cleanup_refuses_image_after_owner_readback_changes(tmp_path: Path) -> None:
    result, state = _run_cleanup_fault(tmp_path, "cleanup_image_owner_changed")
    calls = _docker_call_argvs(state)
    assert result.returncode == 1, state
    assert b"Cleanup refused image tag whose ownership changed" in result.stderr
    assert not any(argv[:2] == ["image", "rm"] for argv in calls), calls
    assert state["container"] is None and state["volume"] is None
    assert state["image"] is not None
    assert list((tmp_path / "scratch").iterdir()) == []
