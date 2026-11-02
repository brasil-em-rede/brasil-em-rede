from pathlib import Path
from unittest.mock import MagicMock

import pytest

from bracc_etl import linking_hooks


@pytest.fixture
def hook_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    repo_root = tmp_path / "repo"
    scripts = repo_root / "scripts"
    scripts.mkdir(parents=True)
    fake_module = repo_root / "etl" / "src" / "bracc_etl" / "linking_hooks.py"
    fake_module.parent.mkdir(parents=True)
    fake_module.touch()
    monkeypatch.setattr(linking_hooks, "__file__", str(fake_module))
    return scripts


def test_missing_second_cnpj_script_fails_before_any_session(hook_tree: Path) -> None:
    (hook_tree / "link_partners_probable.cypher").write_text(
        "MATCH (n) RETURN n;", encoding="utf-8"
    )
    driver = MagicMock()

    with pytest.raises(RuntimeError, match="link_persons.cypher"):
        linking_hooks.run_post_load_hooks(
            driver=driver, source="cnpj", neo4j_database="neo4j", linking_tier="full"
        )

    driver.session.assert_not_called()


def test_empty_script_fails_without_opening_session(hook_tree: Path) -> None:
    (hook_tree / "link_persons.cypher").write_text("  \n", encoding="utf-8")
    driver = MagicMock()

    with pytest.raises(RuntimeError, match="link_persons.cypher"):
        linking_hooks.run_post_load_hooks(
            driver=driver, source="tse", neo4j_database="neo4j", linking_tier="full"
        )

    driver.session.assert_not_called()


def test_community_tier_skips_script_validation(hook_tree: Path) -> None:
    driver = MagicMock()

    linking_hooks.run_post_load_hooks(
        driver=driver, source="cnpj", neo4j_database="neo4j", linking_tier="community"
    )

    driver.session.assert_not_called()


def test_valid_cnpj_scripts_run_in_configured_order(hook_tree: Path) -> None:
    (hook_tree / "link_partners_probable.cypher").write_text(
        "MATCH (n) RETURN n;", encoding="utf-8"
    )
    (hook_tree / "link_persons.cypher").write_text("MATCH (p) RETURN p;", encoding="utf-8")
    session = MagicMock()
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = session

    linking_hooks.run_post_load_hooks(
        driver=driver, source="cnpj", neo4j_database="neo4j", linking_tier="full"
    )

    assert [call.args[0] for call in session.run.call_args_list] == [
        "MATCH (n) RETURN n", "MATCH (p) RETURN p"
    ]


def test_unconfigured_source_is_noop(hook_tree: Path) -> None:
    driver = MagicMock()

    linking_hooks.run_post_load_hooks(
        driver=driver, source="unknown", neo4j_database="neo4j", linking_tier="full"
    )

    driver.session.assert_not_called()
