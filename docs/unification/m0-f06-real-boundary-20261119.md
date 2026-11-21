# F06 boundary proof — 2026-11-19

Registro do ensaio sintético de 2026-11-19. Nenhum arquivo de produção foi alterado. O ensaio usa somente CSV fictício servido por HTTP em loopback, o fluxo F05/F06 integrado e Neo4j descartável de propriedade exclusiva desta execução.

## Resultado verificado

- O caminho HTTP → manifesto F05 → `build_extraction_manifest` / `open_acquired_extraction` → `Pipeline.run` → loader Neo4j foi coberto com dados sintéticos. A segunda ingestão reutiliza checkpoint/origem adquiridos, após desligar o servidor HTTP. O teste consulta o grafo após a repetição e lê métricas persistidas dos dois `IngestionRun`; não chama novamente `Acquisition.acquire_pages`.
- `FilterPlan` explícito que filtra a zero continua aceito conforme #68, com `loaded` persistido e contadores zero. Campo ausente falha com o diagnóstico específico `filter field is missing`, carga não invocada e consulta ao `fund_id` sintético realmente servido.
- CSV somente com cabeçalho atravessa `Pipeline.run`, loader e persistência como zero genuíno.
- Negativo same-length exige `artifact digest mismatch`. O teste distinto `declared-length-after-bridge` injeta, explicitamente em teste, comprimento declarado inconsistente depois da construção/abertura da bridge; bytes e digest permanecem válidos e a asserção exige `artifact byte length mismatch`. Não é alegada falha de EOF natural nem alteração de código de produção.
- Integridade negativa consulta o `fund_id` real servido, comprova `load_extraction` não invocado, cursores fechados/em `falha`, `quality_fail` e métricas de carga zero.
- Falha determinística não transitória no lote final e caso transitório esgotado são casos distintos; ambos comprovam lote anterior confirmado, contadores parciais e `quality_fail`. Erro transitório é injetado na fronteira de transação do loader, não é chamado falha real do servidor.

## Execução e recursos

O runner seleciona o intérprete com `BRACC_TEST_PYTHON` (padrão `python3`), dependências existentes com `BRACC_TEST_DEPS_DIR`, raiz de evidência com `EVIDENCE_ROOT` e limite de prontidão com `BRACC_NEO4J_READY_TIMEOUT`. Não instala dependências. Cada execução cria diretório exclusivo. `etl/src` vem antes das dependências; o preflight exige que `bracc_etl`, `base`, `extraction`, `loader` e a bridge resolvam dentro dessa raiz. Também registra HEAD e SHA-256 dos três arquivos selecionados. No ensaio de 2026-11-19, uma cópia estranha de `bracc_etl` estava presente no diretório de dependências; o preflight confirmou que os módulos críticos foram resolvidos a partir da worktree real, não que a cópia estranha tenha sido selecionada ou importada.

O runner verifica imagem cacheada `sha256:5cce41f4e2a2f69ac65b91769fd4e985668a8fe86e0e8c187830cc4de37722e9`. O inspect registra volume nomeado `/data` e volume anônimo da imagem em `/logs`; cleanup remove somente o contêiner verificado via `docker rm -f -v` e exige readback nativo de ausência do contêiner e de ambos os volumes. Consultas Docker com erro não contam como ausência. Falha de prontidão interrompe antes de pytest.

No ensaio de 2026-11-19, a execução final do runner entregue registrou **8 passed, zero skips/deseleções**, exit `0`, em `.hermes-run/reports/m0-continuous-20261119/f06-boundary/repair-2/final-delivery/run-20261119T213930Z-kWpEFVwL/logs/`. O desafio separado com cópia estrangeira presente no diretório de dependências está em `repair-2/foreign-binding/run-20261119T213519Z-mP28XBiY/logs/`; não é a execução final. No runner final, os imports críticos apontaram para a worktree selecionada. O JUnit inclui readback dos nós/arestas após a reingestão e métricas dos dois runs. A origem HTTP foi requisitada uma vez, antes de desligar o servidor; segunda passagem reutilizou aquisição/checkpoint existente.

Suíte offline relevante: **104 passed**. Ruff, mypy, `bash -n` e `git diff --check`: PASS. Mutante de digest privado no checkout isolado fez o novo negativo same-length falhar com `DID NOT RAISE`; os outros sete testes passaram. A cópia de `extraction.py` foi restaurada byte a byte: SHA-256 `09b7e8ce0e6c04f6ca554e794f08e08ebb781dc2a3b89d24905afd025c4ce91f` em ambas as cópias.

Desafio de guard incorreto, também em checkout isolado: falha artificial `review-only unrelated early refusal` antes da validação. **Todos os três negativos visados falharam por mensagem esperada diferente** (`filter field is missing`, `artifact digest mismatch`, `artifact byte length mismatch`). Execução terminou com 8 falhas no total, como esperado para essa mutação; cleanup nativo passou (`cleanup_failed=0`). Recibo: `repair-2/wrong-guard-challenge/run-20261119T213614Z-vCcBAH2B/logs/`.

## Pendências e limites

A revisão e a publicação são etapas distintas deste ensaio. Evidência sintética não é aceite integral de #7. O ensaio não envolveu dados reais nem alterações de código de produção e não autoriza merge ou deploy.
