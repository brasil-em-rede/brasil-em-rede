# U05 — diário durável de envelopes de evidência

Este recorte grava somente `ValidatedFactEnvelope` já validado pelo contrato #70. O produtor fornece `FactEvent.event_id` explicitamente; `event_identity(event)` identifica a cadeia. UUIDs diferentes mantêm eventos distintos mesmo com o mesmo par, ano, valor ou hash de observação. O diário não infere identidade canônica nem resolve entidades.

`EvidenceStore.append` relê `canonical_bytes` com `read_fact_envelope`, serializa os DTOs tipados fornecidos e os DTOs lidos com `serialize_fact_envelope` do contrato #70 e compara essas representações antes de abrir a transação. Também exige que os bytes fornecidos sejam os bytes canônicos produzidos pelo leitor. Isso preserva campos opcionais omitidos aceitos pelo contrato e evita depender da igualdade Python (`True == 1`) ou comparar o serializer diretamente com o JSON bruto. Cada revisão guarda seu envelope canônico completo, inclusive `FactEvent`, `ObservationRevision` e referências de evidência de topo; `envelopes(event)` recupera esses envelopes e `event_history(event)` a cadeia validada por `apply_revision`. Replay byte a byte é idempotente; conteúdo divergente para o mesmo evento/observação é recusado. A conexão psycopg autenticada pertence ao chamador, que controla a transação.

O vínculo confere o artefato primário e cada `EvidenceRef` de topo, inclusive caminhos de atributo e relação, contra `control.artifact → partition → dataset → source`, UUID, hash e metadados de contrato. É vínculo de metadados do catálogo, não leitura dos bytes físicos nem prova de custódia. O modelo de chaves estrangeiras do catálogo limita quais linhagens podem ser combinadas; uma referência estrangeira não permite variar fonte e dataset independentemente. Não se afirma aceitação de combinações que o contrato e o catálogo existentes não autorizem.

A migration 006 contém apenas DDL do diário e de seus bloqueios/guardas; a cobertura aplica 001–006 somente a PostgreSQL sintético novo e exclusivo. Não faz backfill, não altera dados existentes e não autoriza migração operacional. O papel de runtime ensaiado é um login não superusuário: recebe `SELECT`/`INSERT` em `evidence.journal` e `UPDATE` em `evidence.event_lock` para `SELECT … FOR UPDATE`; `UPDATE`/`DELETE`/`TRUNCATE` do diário são recusados com SQLSTATE `42501`. As guardas adicionais não protegem contra dono ou superusuário.

## Ensaio PostgreSQL sintético

Execute `bash etl/tests/control/evidence_journal.sh` a partir da raiz do repositório. O entrypoint exige `TMPDIR` ou `BRACC_EVIDENCE_TMPDIR`; usa `BRACC_TEST_PYTHON` quando definido e, caso contrário, `etl/.venv/bin/python`. O interpretador selecionado precisa importar `psycopg`. Prepare scratch exclusivo e use a imagem-base PostgreSQL no digest fixado pelo Dockerfile já disponível localmente.

```bash
uv sync --project ./etl --extra control --group dev --frozen
export TMPDIR="<scratch-autorizado>"
export BRACC_TEST_PYTHON="$PWD/etl/.venv/bin/python"
bash etl/tests/control/evidence_journal.sh
```

O runner distingue presença, `NotFound` reconhecido e falha inesperada nas inspeções antes do build. Recusa colisões; só considera a imagem adquirida após build bem-sucedido e readback do ID, tag e `bracc.owner`. Só considera o container adquirido após readback do ID, nome, etiqueta e inventário de mounts. A porta é efêmera e vinculada a `127.0.0.1`. O trap é instalado antes de criar scratch e usa o interpretador selecionado para escolher a porta.

Na limpeza, o runner inspeciona a identidade ainda pertencente ao ensaio, remove apenas o container pelo ID e a tag exata da imagem, e confirma ausência nativa do container, tag e cada volume listado em `Mounts`. Erro de daemon, remoção, verificação ou scratch impede retorno verde. Se o ensaio falhou antes, o status primário é preservado e a falha de limpeza é diagnosticada separadamente. Não há `prune` nem remoção por prefixo.

A execução PostgreSQL corrigida de 20 de novembro de 2026 retornou 0 com `catalog_refusals=12`: 10 recusas isoladas de catálogo e 2 conflitos de replay; também registrou `rollback_after_insert=True`, `denied_mutations=3`, `exact_replay_lock=True` e `divergent_revision_race=PASS`. O container, a tag, o volume anônimo de `/var/lib/postgresql/data` e o scratch do ensaio tiveram ausência verificada depois. IDs, porta e PIDs são locais à execução, não parte do contrato.

A cobertura é um diário interno, não integração com `Pipeline.run`, projeção de alegações públicas, outbox, aquisição, backfill ou custódia física. Os ensaios e a migration não alteram bancos existentes. As capturas RED da implementação original não foram preservadas; a correção posterior tem regressões próprias e uma mutação independente de produção para sensibilidade, sem reclassificar a cronologia histórica como TDD.
