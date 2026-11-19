from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from bracc.services.neo4j_service import (
    CypherLoader,
    ensure_schema,
    execute_query,
    execute_query_single,
)


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    CypherLoader.clear_cache()


class TestCypherLoader:
    def test_loads_cypher_file(self, tmp_path: Path) -> None:
        query_file = tmp_path / "test_query.cypher"
        query_file.write_text("MATCH (n) RETURN n LIMIT $limit")

        import bracc.services.neo4j_service as mod

        original = mod.QUERIES_DIR
        mod.QUERIES_DIR = tmp_path
        try:
            result = CypherLoader.load("test_query")
            assert result == "MATCH (n) RETURN n LIMIT $limit"
        finally:
            mod.QUERIES_DIR = original

    def test_caches_loaded_queries(self, tmp_path: Path) -> None:
        query_file = tmp_path / "cached.cypher"
        query_file.write_text("RETURN 1")

        import bracc.services.neo4j_service as mod

        original = mod.QUERIES_DIR
        mod.QUERIES_DIR = tmp_path
        try:
            CypherLoader.load("cached")
            query_file.write_text("RETURN 2")
            assert CypherLoader.load("cached") == "RETURN 1"
        finally:
            mod.QUERIES_DIR = original

    def test_raises_on_missing_file(self) -> None:
        with pytest.raises(FileNotFoundError, match="Query file not found"):
            CypherLoader.load("nonexistent_query_xyz")

    def test_clear_cache_works(self, tmp_path: Path) -> None:
        query_file = tmp_path / "clearable.cypher"
        query_file.write_text("RETURN 1")

        import bracc.services.neo4j_service as mod

        original = mod.QUERIES_DIR
        mod.QUERIES_DIR = tmp_path
        try:
            CypherLoader.load("clearable")
            assert "clearable" in CypherLoader._cache
            CypherLoader.clear_cache()
            assert "clearable" not in CypherLoader._cache
        finally:
            mod.QUERIES_DIR = original


class TestEnsureSchema:
    @pytest.mark.anyio
    async def test_shipped_schema_comments_are_removed_before_statement_split(self) -> None:
        driver = MagicMock()
        session = AsyncMock()
        session_context = MagicMock()
        session_context.__aenter__ = AsyncMock(return_value=session)
        session_context.__aexit__ = AsyncMock(return_value=None)
        driver.session.return_value = session_context

        await ensure_schema(driver)

        statements = [call.args[0] for call in session.run.call_args_list]
        assert statements
        assert all(not statement.startswith("cross-label") for statement in statements)
        assert any(
            "CREATE CONSTRAINT person_entity_id_unique" in statement for statement in statements
        )
        assert any("CREATE CONSTRAINT company_cnpj_unique" in statement for statement in statements)


class TestExecuteQuery:
    @pytest.mark.anyio
    async def test_execute_query_returns_records(self, tmp_path: Path) -> None:
        query_file = tmp_path / "test.cypher"
        query_file.write_text("MATCH (n) RETURN n")

        import bracc.services.neo4j_service as mod

        original = mod.QUERIES_DIR
        mod.QUERIES_DIR = tmp_path

        mock_record = MagicMock()
        mock_result = AsyncMock()
        mock_result.__aiter__ = lambda self: self
        mock_result.__anext__ = AsyncMock(side_effect=[mock_record, StopAsyncIteration])

        session = AsyncMock()
        session.run.return_value = mock_result

        try:
            records = await execute_query(session, "test", {"key": "value"})
            session.run.assert_called_once_with("MATCH (n) RETURN n", {"key": "value"}, timeout=15)
            assert records == [mock_record]
        finally:
            mod.QUERIES_DIR = original

    @pytest.mark.anyio
    async def test_execute_query_single_returns_record(self, tmp_path: Path) -> None:
        query_file = tmp_path / "single.cypher"
        query_file.write_text("RETURN 1 AS ok")

        import bracc.services.neo4j_service as mod

        original = mod.QUERIES_DIR
        mod.QUERIES_DIR = tmp_path

        mock_record = MagicMock()
        mock_result = AsyncMock()
        mock_result.single = AsyncMock(return_value=mock_record)

        session = AsyncMock()
        session.run.return_value = mock_result

        try:
            record = await execute_query_single(session, "single")
            assert record == mock_record
        finally:
            mod.QUERIES_DIR = original
