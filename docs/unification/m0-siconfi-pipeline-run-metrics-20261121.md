# SICONFI: métricas implícitas e recibos de execução

## Escopo

Este teste de integração exercita `SiconfiPipeline.run()` com um CSV sintético gravado em `tmp_path`. O caminho nativo executa extração, transformação e carga. `project_siconfi` descobre as métricas pelo contexto de `Pipeline.run`; o teste não injeta `metrics=` no projetor.

A fixture reutiliza o driver e as constraints de `tests.dimensions.test_f09_metrics_boundary`. O runner cria um Neo4j descartável identificado por execução e seleciona explicitamente os dois módulos de integração. O teste alinha `NEO4J_DATABASE=neo4j` apenas no escopo de `monkeypatch`, pois o loader SICONFI lê essa variável.

## Fixture local

O CSV contém duas linhas, para `receita` (100) e `despesa` (200), com município `3550308`, UF `SP`, exercício textual `2023`, unidade `SICONFI/BRL` e órgão sintético `SP/synthetic/ug/000123/fixture-v1`. A versão territorial `f09-fixture-v1` também é sintética. `000123` deve permanecer texto em todo o percurso.

Um crosswalk local contém as entradas literais de território, órgão, unidade e exercício. Seu SHA-256 é calculado sobre a serialização JSON canônica; isso verifica integridade e associação somente à fixture local. Não comprova vocabulário oficial, validade do exercício SICONFI 2023 nem semântica econômica da unidade.

## Oráculo físico

A expectativa é independente das saídas de transformação e projeção do pipeline. A primeira chamada cria dois nós `MunicipalFinance` e quatro dimensões compartilhadas (`Territory`, `PublicBody`, `Unit`, `FiscalYear`): seis nós de negócio e oito relações, quatro por fato. Não há nós `Person` ou `Company`.

As propriedades completas dos fatos incluem `finance_id` literais `ea16d71178b737dc` e `3bf4b4006273cb29`, montantes `100.0` e `200.0`, os códigos textuais e os estados `matched_local`/`projected`. O teste compara propriedades completas de nós e relações, incluindo endpoints e mapas de propriedades vazios das relações.

O teste mantém o conjunto físico `first_relations` separado de `expected_relations`, um oráculo literal independente. Ele compara a primeira leitura física ao oráculo imediatamente; no replay e após recusa, compara novamente com o primeiro snapshot e com o mesmo oráculo.

### Métricas públicas esperadas

`direct_updates`, `properties_set` e `verified_rows` devem permanecer nos vetores independentes: 41/4/0, 59/36/0 e 4/4/0. A suíte de aprovação confere cada campo completo no grafo de métricas e em cada `IngestionRun`; recibos anteriores devem permanecer integralmente iguais após replay e recusa, incluindo timestamps e campos adicionais. O recibo novo precisa corresponder ao `run_id` exato da chamada. Não derive expectativas de resultados observados.

Os vetores completos e seus campos são mantidos como literais independentes no teste. Em cada recibo, os contadores são comparados diretamente, sem valor padrão que esconda campo ausente.

`verified_rows=4` soma verificações dos dois fatos no loader e no projetor; não representa quatro registros de entrada distintos. `rows_in=0` é a semântica legada atual, não uma reconciliação do CSV. O projetor não usa o loader de relacionamentos; por isso, `relationships_resolved=0` coexiste com as oito relações físicas.

O replay deve preservar o grafo de negócio e o recibo anterior, criar novo `run_id` e registrar operações idempotentes de propriedades em `properties_set=36`. Cada `IngestionRun` é um nó físico fora dos contadores de carga; totais globais esperados após inicial, replay e falha são 7, 8 e 9 nós, sempre com oito relações.

## Recusa de checksum

Na terceira chamada, o teste mantém as entradas do crosswalk e troca somente `artifact_sha256` por 64 zeros. `SiconfiPipeline.run()` deve levantar `CrosswalkError("artifact_sha256 does not match canonical entries")` antes da carga. O novo recibo deve ser `quality_fail`, guardar o diagnóstico literal e relatar zero linhas carregadas e métricas zeradas. Os dois recibos `loaded` anteriores e o grafo de negócio devem permanecer iguais.

## Evidência desta entrega

Estado anterior à tentativa desta entrega: correções nos três caminhos aplicadas; nenhum resultado de Neo4j, recibo persistido, execução do runner ou gate de qualidade desta correção foi afirmado. A validação estática e offline consta no handoff privado; teste de integração com o fixture live continua pendente. Expectativa estática não é prova de execução. Se o pipeline divergir dos valores derivados, preservar o recibo e investigar sem enfraquecer o oráculo.

## Execução e limites

Execute, a partir da raiz do worktree, o runner `etl/tests/integration/m0_f09_metrics_boundary.sh` com `BRACC_TEST_PYTHON`, `TMPDIR` e `EVIDENCE_ROOT` exclusivos. O runner verifica a imagem Neo4j local fixada, cria volume e container com identidade própria, restringe a porta a loopback, registra inventário e confirma a remoção dos recursos identificados. Não use `docker prune` nem serviço compartilhado.

A prova não cobre aquisição de fontes, crosswalk oficial, vínculo/identidade, publicação, RAIS, retries de lote, migrações ou carga operacional. Divergência entre os vetores derivados e a execução deve ser preservada e investigada; não ajuste expectativas apenas para obter resultado verde.
