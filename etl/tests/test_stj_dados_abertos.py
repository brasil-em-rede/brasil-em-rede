from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from bracc_etl.pipelines.stj_dados_abertos import StjPipeline
from tests.neo4j_mocks import make_driver


def _case_id(case_class: str, number: str, year: str) -> str:
    return hashlib.sha256(f"stj:{case_class}:{number}:{year}".encode()).hexdigest()[:16]


def test_extract_transforms_stj_csv_and_loads_expected_keys(tmp_path, monkeypatch) -> None:
    source = tmp_path / "stj_dados_abertos"
    source.mkdir()
    (source / "decisoes.csv").write_text(
        "classe,numero,ano,relator,tipo_decisao,data_decisao,assunto,uf_origem\n"
        " REsp , 123 ,2024, JOÃO DA SILVA ,Acórdão,2024-02-03,tema,SP\n"
        "AgInt,456,2023,,,2023-01-02,outro,RJ\n"
        ",789,2022,Nome,,,,\n",
        encoding="utf-8",
    )
    pipeline = StjPipeline(driver=make_driver(), data_dir=str(tmp_path), limit=1)
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline._raw) == 3
    assert pipeline.cases == [
        {
            "case_id": _case_id("REsp", "123", "2024"),
            "case_class": "REsp",
            "case_number": "123",
            "year": "2024",
            "rapporteur": "JOAO DA SILVA",
            "decision_type": "Acórdão",
            "decision_date": "2024-02-03",
            "subject": "tema",
            "origin_state": "SP",
            "court": "STJ",
            "source": "stj_dados_abertos",
        }
    ]
    assert pipeline.rapporteur_rels == [
        {"source_key": "JOAO DA SILVA", "target_key": _case_id("REsp", "123", "2024")}
    ]

    calls = []
    monkeypatch.setattr(
        "bracc_etl.pipelines.stj_dados_abertos.Neo4jBatchLoader",
        lambda driver: type(
            "Loader",
            (),
            {
                "load_nodes": lambda self, *args, **kwargs: calls.append(("nodes", args, kwargs)),
                "run_query_with_retry": lambda self, *args: calls.append(("query", args)),
            },
        )(),
    )
    pipeline.load()
    assert calls[0] == ("nodes", ("LegalCase", pipeline.cases), {"key_field": "case_id"})
    assert calls[1][0] == "query"
    assert calls[1][1][1] == pipeline.rapporteur_rels


def test_stj_limit_preserves_first_case_identity_and_rapporteur_target() -> None:
    first_id = _case_id("HC", "101", "2024")
    pipeline = StjPipeline(driver=make_driver(), limit=1)
    pipeline._raw = pd.DataFrame(
        [
            {
                "classe": "HC", "numero": "101", "ano": "2024",
                "relator": " MARIA DE SOUZA ", "tipo_decisao": "Acórdão",
                "data_decisao": "2024-01-01", "assunto": "primeiro", "uf_origem": "SP",
            },
            {
                "classe": "REsp", "numero": "202", "ano": "2025",
                "relator": "OUTRO RELATOR", "tipo_decisao": "Decisão",
                "data_decisao": "2025-02-02", "assunto": "segundo", "uf_origem": "RJ",
            },
        ]
    )

    pipeline.transform()

    assert [case["case_id"] for case in pipeline.cases] == [first_id]
    assert pipeline.cases[0]["case_number"] == "101"
    assert pipeline.rapporteur_rels == [
        {"source_key": "MARIA DE SOUZA", "target_key": first_id}
    ]


def test_extract_missing_stj_file_raises(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="STJ CSV not found"):
        StjPipeline(driver=make_driver(), data_dir=str(tmp_path)).extract()


def test_stj_deduplicates_identity_and_skips_rows_without_required_identity() -> None:
    pipeline = StjPipeline(driver=make_driver())
    pipeline._raw = pd.DataFrame(
        [
            {
                "classe": "HC",
                "numero": "1",
                "ano": "2024",
                "relator": "",
                "tipo_decisao": "",
                "data_decisao": "",
                "assunto": "",
                "uf_origem": "",
            },
            {
                "classe": "HC",
                "numero": "1",
                "ano": "2024",
                "relator": "Outro",
                "tipo_decisao": "",
                "data_decisao": "",
                "assunto": "",
                "uf_origem": "",
            },
            {
                "classe": "",
                "numero": "2",
                "ano": "2024",
                "relator": "",
                "tipo_decisao": "",
                "data_decisao": "",
                "assunto": "",
                "uf_origem": "",
            },
        ]
    )
    pipeline.transform()
    assert [row["case_id"] for row in pipeline.cases] == [_case_id("HC", "1", "2024")]
    assert pipeline.rapporteur_rels == [
        {"source_key": "OUTRO", "target_key": _case_id("HC", "1", "2024")}
    ]
