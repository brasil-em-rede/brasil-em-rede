# Publicação órfã no fluxo U03-F05

Este teste usa PostgreSQL descartável e dados sintéticos. Não conecta a fontes nem altera ambiente compartilhado.

A regressão exercita `publish_partition` real. A falha injetada ocorre após publicar bytes imutáveis e antes do commit catalogado; o teste exige objeto órfão verificável por SHA-256, ausência de linhas parciais e preservação do release ativo. Também exige recusa de promoção com outbox pendente, ACK sob `bracc_control_projector`, promoção válida, recusa de CAS incorreto e de token expirado.

Execute `bash etl/tests/control/orphan_publication.sh`. Requer Docker e Python com `psycopg` disponível via `BRACC_TEST_PYTHON` ou `/usr/bin/python3.12`. O script cria container exclusivo com nome aleatório, aplica as migrações do repositório e remove apenas o container criado.

Escopo: teste de controle sintético; nenhuma aquisição, carga compartilhada, reset, seed operacional, migração real, backfill ou decisão de domínio.
