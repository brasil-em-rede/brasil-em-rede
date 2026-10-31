from __future__ import annotations

import pandas as pd
import pytest

from bracc_etl.pipelines.tesouro_emendas import TesouroEmendasPipeline
from tests.neo4j_mocks import make_driver


def _row(
    ob: str,
    date: str = "42005",
    value: str = "1.234,56",
    cnpj: str = "04.252.011/0001-10",
) -> dict[str, str]:
    return {
        "OB": ob,
        "Data": date,
        "Ano": "2024",
        "Mês": "3",
        "Nome Emenda": "Emenda",
        "Transferência Especial": "Sim",
        "Categoria Econômica Despesa": "Corrente",
        "Valor": value,
        "CNPJ do Favorecido": cnpj,
        "Nome Favorecido": " Empresa Ágil ",
    }


def test_extract_transforms_tesouro_semicolon_latin1_and_loads_expected_keys(
    tmp_path, monkeypatch
) -> None:
    source = tmp_path / "tesouro_emendas"
    source.mkdir()
    (source / "emendas_tesouro.csv").write_bytes(
        (
            "OB;Data;Ano;Mês;Nome Emenda;Transferência Especial;Categoria Econômica Despesa;"
            "Valor;CNPJ do Favorecido;Nome Favorecido\n"
            "OB-1;42005;2024;3;Emenda;Sim;Corrente;1.234,56;04.252.011/0001-10;Empresa Ágil\n"
            "OB-2;data;2023;2;Outra;Não;Capital;inválido;bad;Favorecido\n"
        ).encode("latin-1")
    )
    pipeline = TesouroEmendasPipeline(driver=make_driver(), data_dir=str(tmp_path))
    pipeline.extract()
    pipeline.transform()
    assert len(pipeline._raw) == 2
    assert pipeline.transfers == [
        {
            "transfer_id": "transfer_tesouro_OB-1",
            "ob": "OB-1",
            "date": "2015-01-01",
            "year": "2024",
            "month": "3",
            "amendment_type": "Emenda",
            "special_transfer": "Sim",
            "economic_category": "Corrente",
            "value": 1234.56,
            "source": "tesouro_emendas",
        },
        {
            "transfer_id": "transfer_tesouro_OB-2",
            "ob": "OB-2",
            "date": "data",
            "year": "2023",
            "month": "2",
            "amendment_type": "Outra",
            "special_transfer": "Não",
            "economic_category": "Capital",
            "value": 0.0,
            "source": "tesouro_emendas",
        },
    ]
    assert pipeline.companies == [{"cnpj": "04252011000110", "razao_social": "EMPRESA AGIL"}]
    assert pipeline.transfer_rels == [
        {"source_key": "transfer_tesouro_OB-1", "target_key": "04252011000110"}
    ]

    calls = []
    monkeypatch.setattr(
        "bracc_etl.pipelines.tesouro_emendas.Neo4jBatchLoader",
        lambda driver: type(
            "Loader",
            (),
            {
                "load_nodes": lambda self, *args, **kwargs: calls.append(("nodes", args, kwargs)),
                "run_query": lambda self, *args: calls.append(("query", args)),
            },
        )(),
    )
    pipeline.load()
    assert calls[0] == ("nodes", ("Payment", pipeline.transfers), {"key_field": "transfer_id"})
    assert calls[1] == ("nodes", ("Company", pipeline.companies), {"key_field": "cnpj"})
    assert calls[2][1][1] == pipeline.transfer_rels


def test_tesouro_missing_file_raises(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="Tesouro Emendas CSV not found"):
        TesouroEmendasPipeline(driver=make_driver(), data_dir=str(tmp_path)).extract()


def test_tesouro_limit_identity_dedup_and_invalid_document_skip() -> None:
    pipeline = TesouroEmendasPipeline(driver=make_driver(), limit=2)
    pipeline._raw = pd.DataFrame([_row("A"), _row("A", cnpj="bad"), _row("B")])
    pipeline.transform()
    assert [row["transfer_id"] for row in pipeline.transfers] == ["transfer_tesouro_A"]
    assert pipeline.companies == [{"cnpj": "04252011000110", "razao_social": "EMPRESA AGIL"}]
    assert pipeline.transfer_rels == [
        {"source_key": "transfer_tesouro_A", "target_key": "04252011000110"}
    ]


def test_tesouro_limit_stops_after_two_distinct_competing_rows() -> None:
    pipeline = TesouroEmendasPipeline(driver=make_driver(), limit=2)
    pipeline._raw = pd.DataFrame([_row("A"), _row("B"), _row("C")])

    pipeline.transform()

    assert [row["transfer_id"] for row in pipeline.transfers] == [
        "transfer_tesouro_A",
        "transfer_tesouro_B",
    ]
    assert pipeline.transfer_rels == [
        {"source_key": "transfer_tesouro_A", "target_key": "04252011000110"},
        {"source_key": "transfer_tesouro_B", "target_key": "04252011000110"},
    ]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1.234,56", 1234.56),
        ("1.234.567,89", 1234567.89),
        ("-1.234,56", -1234.56),
        ("1234,56", 1234.56),
        ("1234.56", 1234.56),
        ("1234", 1234.0),
        ("inválido", 0.0),
    ],
)
def test_tesouro_parses_brl_and_compatible_numeric_values(raw: str, expected: float) -> None:
    pipeline = TesouroEmendasPipeline(driver=make_driver())
    pipeline._raw = pd.DataFrame([_row("A", value=raw)])

    pipeline.transform()

    assert pipeline.transfers[0]["value"] == expected


def test_tesouro_skips_rows_without_ob() -> None:
    pipeline = TesouroEmendasPipeline(driver=make_driver())
    pipeline._raw = pd.DataFrame([_row(" ")])
    pipeline.transform()
    assert pipeline.transfers == []
    assert pipeline.companies == []
    assert pipeline.transfer_rels == []
