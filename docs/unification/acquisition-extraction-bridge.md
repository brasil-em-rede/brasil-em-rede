# Ponte delimitada F05 → F06

`bracc_etl.acquisition.extraction_bridge` converte um checkpoint local F05 em um manifesto F06 separado; não altera o manifesto de aquisição nem os arquivos baixados.

## Uso

Durante `Pipeline.run()`, `produce_extraction()` deve usar o `self.run_id` recém-gerado naquele mesmo ciclo. Construa os vínculos explícitos `population_id -> artifact_id -> (filename, schema_id, schema_version)` e um mapa `filename -> URL original` para as páginas atribuídas. Então chame `build_extraction_manifest(root, acquisition_manifest, adapter, run_id=..., source_id=..., source_url=..., assignments=..., page_urls=..., output_path=...)` e `open_acquired_extraction(...)`. O segundo método delega diretamente ao verificador autoritativo `open_extraction`; o resultado e os cursores emitidos são devolvidos a `Pipeline.run()` para transformação e carga estruturadas.

Os valores URL de `page_urls` são comparados por SHA-256 com `source_sha256` do checkpoint F05 e não são copiados para o manifesto F06. A identidade de execução, fonte e adapter é declarada pelo chamador e comparada com a política do adapter; não é inferida da contagem de páginas. A ponte só aceita nomes de arquivo planos, atribuições sem duplicidade, política de população exata, checkpoint com hash/tamanho válidos, arquivo regular aberto sem seguir symlink e saída nova como filha direta do diretório confiável. Os bytes são lidos em blocos e comparados com hash e tamanho do checkpoint. Caminho ausente, incompleto, adulterado, sem proveniência, com schema divergente ou saída existente/fora da raiz falha fechado.

## Evidência e limites

O teste de integração usa `Acquisition.acquire_pages` real contra um servidor HTTP `127.0.0.1` efêmero e fixture sintética, a ponte real, `open_extraction` real e `Pipeline.run()` real. Somente o driver/efeito externo de carga é falso. O teste confirma os bytes originais F05, manifesto F06 distinto, identidade com o run real e estado carregado; há também casos para EOF/CSV vazio observado, alteração de mesmo tamanho, proveniência URL, schema, tamanho do checkpoint, traversal e escape de saída.

Atribuir população, artefato e schema é uma afirmação explícita do chamador, não validação semântica do conteúdo ou autoridade da fonte. A prova de hash confirma identidade dos bytes contra o checkpoint, não veracidade da origem nem correção do parser. Consumidores ainda precisam adotar a integração; este módulo não é um hook universal dos pipelines. O consumidor F06 atual exige cobertura de artefatos conforme sua `SourceAdapterSpec`, inclusive para populações opcionais declaradas; ausência de export opcional não é sintetizada como evidência. Uma exportação vazia é distinta: precisa existir como arquivo com schema/header e EOF observados pelo cursor F06.

Sem aquisição externa, dados reais, banco, escrita de produção ou publicação.
