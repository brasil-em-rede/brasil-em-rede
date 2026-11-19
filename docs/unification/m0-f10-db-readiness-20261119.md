# F10 — readiness DB-backed e correção de parser (1/2)

## Resultado

A correção 1/2 remove linhas que começam com `//` **antes** de dividir statements por `;` em `ensure_schema` e no `_split_statements` de ETL. Não houve mudança na semântica do schema, classificação/linkagem, matcher ou política de domínio. A transformação é linha-completa; não remove `//` dentro de strings. Ordem dos statements e propagação de erros de `session.run` permanecem inalteradas.

A falha de startup de F10 foi corrigida e validada de ponta a ponta em API real + Neo4j descartável sob `APP_ENV=production`. O teste não usa fakes para respostas, fixture, banco, readiness ou probe. O probe é o arquivo shipped copiado byte a byte do worktree e executado no container API.

## TDD e regressão

- RED API: `api/tests/unit/test_neo4j_service.py::TestEnsureSchema::test_shipped_schema_comments_are_removed_before_statement_split` carrega o `schema_init.cypher` shipped via `CypherLoader`; encontrou statement iniciado por `cross-label` e falhou por assertion comportamental, não setup.
- RED ETL: `etl/tests/test_linking_hooks.py::test_split_statements_removes_full_line_comments_before_semicolon_split` lê o mesmo schema versionado; encontrou statement iniciado por `cross-label`.
- GREEN focal: API `1 passed`; ETL linking-hooks `6 passed`.
- API unit completo: `232 passed, 1 skipped`; 42 JWT test warnings por chaves de teste curtas.
- F10 profile regression: `9 passed, 2 subtests passed`.
- Mypy strict nos arquivos API tocados: `Success: no issues found in 2 source files`; ETL: mesmo resultado para seus 2 arquivos. O teste integrado F10 também passou mypy strict depois de renomear variáveis locais e ignorar a falta de stubs Docker (`mypy-f10.log`).
- Ruff nos cinco arquivos da alteração, `py_compile` e `git diff --check`: passaram.

## Teste DB-backed completo

Última execução integrada reproduzível, com os oráculos R2 habilitados: **6 passed in 195.58s**.

Na raiz do repositório:
```sh
uv sync --project api --extra dev --locked
PYTHONPATH=api/src F10_RUN_DB_READINESS=1 uv run --project api --extra dev \
  pytest -q -s tests/test_f10_db_readiness.py
```

A asserção da fixture lê a tabela CSV do `cypher-shell` e exige valor inteiro exatamente `1`; saída `count\n10\n` é rejeitada. O catálogo HTTP deve ter IDs públicos únicos, com conjunto exatamente igual ao calculado independentemente da coluna `in_universe_v1=true` no CSV canônico, tanto antes quanto depois do restart.

| Estado | Resposta real observada |
| --- | --- |
| API live `/health` | HTTP 200 `{"status":"ok"}` |
| Banco conectado `/api/v1/meta/health` | HTTP 200 `{"neo4j":"connected"}` |
| Catálogo `/api/v1/meta/sources` | HTTP 200, 168 entradas |
| Empresa sintética `/api/v1/entity/11222333000181` | HTTP 200, Company CNPJ `11222333000181`, nome `Synthetic F10 Company` |
| CNPJ válido sintético ausente `/api/v1/entity/88888888000191` | HTTP 404 `Entity not found`; dígitos verificadores conferidos pelo helper ETL |
| Probe shipped, banco saudável | exit 0 |
| Neo4j parado; API liveness | HTTP 200 |
| Neo4j parado; `/api/v1/meta/health` | HTTP 500; falha real propagada pelo driver e readiness negada |
| Probe shipped, banco parado | nonzero |
| Mesmo container Neo4j reiniciado | readiness HTTP 200; entidade preservada HTTP 200, 168 fontes; probe exit 0 |

Neo4j executado em rede exclusiva, imagem `neo4j:5.26.21-community`, ID `sha256:5cce41f4e2a2f69ac65b91769fd4e985668a8fe86e0e8c187830cc4de37722e9`. API usou imagem candidata local ID `sha256:4c9a285a4887f062488d6ae1ade0197e12d75506cb36b8742efda2f690ff6317`, com pacote do worktree e CSV montados read-only. O container confirmou `bracc.services.neo4j_service.__file__ == /app/bracc/services/neo4j_service.py`; hash do módulo `2ad40c63be3739ccb1d4c1bd0f7741d2d96d938f8be2f5acebeaa01f39e7344f`, igual ao arquivo atual do worktree. Hash do registry `06f9cc0175ca3b57c7a7a251ff3b52ca2dbdfc60da4fd275650751c702b9ee1a`, igual ao worktree. Hash do probe `d9b931cf1523ede6d750327b733ccab06de9b48d355866c07db1bd51cf2c3987`.

API e Neo4j compartilharam uma rede Docker exclusiva. Somente HTTP foi publicado em loopback dinâmico; Bolt não foi publicado no host. `finally` removeu containers, rede e dois anonymous volumes; readback confirmou cada recurso ausente. Não foram tocados serviços compartilhados.

## Arquivos alterados

- `api/src/bracc/services/neo4j_service.py`
- `api/tests/unit/test_neo4j_service.py`
- `etl/src/bracc_etl/linking_hooks.py`
- `etl/tests/test_linking_hooks.py`
- `tests/test_f10_db_readiness.py`
- `docs/unification/m0-f10-db-readiness-20261119.md`

## Evidências

Relatórios operacionais privados preservam preimages, RED/GREEN, logs e JUnit, testes API/ETL, regressão de perfil, mypy, stdout/stderr da integração, resumo DB-backed, source binding e handoff. Os caminhos privados do host não são publicados neste documento.

Validação feita com Python 3.12 em ambiente de desenvolvimento isolado da execução; nenhum caminho de instalação local é requisito deste procedimento reproduzível.

## Limites / awaiting review

Prova sintética local; não certifica credenciais reais, capacidade de produção, bytes/linha, partição representativa, aquisição/carga real, TLS público, deploy ou restore operacional. Não é aceite humano da issue #11. Correção e F10 permanecem `awaiting_review`; sem commit, PR ou publicação.
