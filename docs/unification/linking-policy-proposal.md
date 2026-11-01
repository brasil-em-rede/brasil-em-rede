# F07 — proposta de política mínima de ligação

**Estado: PROPOSTA NÃO APROVADA.** Este documento propõe uma única decisão de domínio ainda não coberta por aprovação: permitir ligação exata de estabelecimentos `Company` por CNPJ completo. A proposta não concede autorização. Separadamente, a autorização contínua já existente do usuário para engenharia M0 reversível permanece vigente — não é revogada por esta proposta — e cobre implementação, commits e PRs somente após decisão de domínio favorável e dependências técnicas prontas, sem novo pedido de início. Não é política vigente, configuração de runtime, prova externa de cadastro nem autorização para escrita no grafo; operações no grafo/dados reais e demais gates operacionais seguem exigindo permissão explícita própria.

## Decisão de domínio solicitada

A recomendação é aprovar ligação exata de estabelecimentos `Company` por CNPJ completo; alternativa: manter as ligações automáticas fechadas. Este é o único ponto submetido à decisão humana. Os parâmetros seguintes são uma proposta técnica fechada, não decisões adicionais item a item. O escopo não inclui `LegalEntity`, órgãos públicos, pessoas, contratos nem relações societárias.

## Binding local, não normativo

`docs/unification/canonical-model.md`, “Identidade da entidade e identidade do registro” e “Resolução, divergências e correções”, é proposta de modelo, não regra aceita nem matcher concreto. `etl/src/bracc_etl/identity/legacy.py::propose` preserva UUIDs e relata colisões; não autoriza fusão. Não há regra positiva de fusão aceita.

Implementação observada em `etl/src/bracc_etl/transforms/document_formatting.py::normalize_cnpj`, `::validate_cnpj`, `::_cnpj_check_digits` e `::classify_document`; fixtures em `etl/tests/test_transforms.py` e `etl/tests/test_alphanumeric_documents.py`. Normalizer remove `.`, `/`, `-`, apenas trims bordas e uppercases; preserva letras/zeros. Validator usa o padrão `[A-Z0-9]{12}[0-9]{2}` e DV; classificador verifica forma, não DV. Isso não estabelece autoridade, cadastro atual nem política. Não reparar identificador nem ignorar DV.

## F07 — propostas de parâmetros e binding não normativos

| Parâmetro | Valor proposto |
|---|---|
| Domínio/namespace | `br:cnpj:establishment`; tipo `Company`; escopo explícito `BR`/estabelecimento |
| Tipo/grão | CNPJ completo de estabelecimento, exatamente 14 caracteres normalizados; nunca raiz CNPJ |
- Proposta de binding: `bracc-cnpj-normalize-v1`, função `etl/src/bracc_etl/transforms/document_formatting.py::normalize_cnpj`.
- Proposta de binding: `bracc-cnpj-checkdigits-v1`, função `etl/src/bracc_etl/transforms/document_formatting.py::validate_cnpj`.
- No bundle da implementação aprovada, fixar ambas as versões propostas ao commit e SHA-256 reais do arquivo código; os rótulos não são versões runtime existentes nem política aceita.
| Papel compatível | Ambas as observações devem sustentar o mesmo papel compatível de estabelecimento empresarial mapeado a `Company`; incompatibilidade explícita: `refused`; papel desconhecido: `pending`. `LegalEntity`, `PublicBody`, pessoa, fundo, administrador, órgão e menção não são intercambiáveis. |
| Escopo temporal | Positivo apenas se ambos os intervalos forem conhecidos, mesma precisão e interseção não vazia; desconhecido ou precisão divergente: `pending`; incompatibilidade explícita: `refused` |
| Proveniência obrigatória | `source_id`, `dataset_id`, versão do contrato da fonte, artifact UUID e SHA-256, locator/tipo, `field_path`, chave/identidade do registro, valor observado, precisão/validade; versões de normalizador, validador e regra quando aplicáveis |
| Igualdade/estados | Igualdade exata da chave qualificada normalizada `(namespace, tipo, escopo, normalize_cnpj(valor))`, com CNPJ completo de 14 caracteres e DV válido; grafias brutas distintas que normalizam à mesma chave podem corresponder. Manter cada valor bruto observado em seu próprio `EvidenceRef`; não exigir identidade da tupla bruta nem igualdade de todos os campos de evidência. Ambas as observações devem sustentar o mesmo papel compatível, com evidência requerida e períodos conhecidos compatíveis para `candidate` revisável, nunca fusão automática. Formato/DV inválido: `refused`, sem chave de ligação; ausente, parcial ou mascarado: `pending`. Papel/temporal obrigatório desconhecido ou precisão incompatível: `pending`; incompatibilidade explícita conhecida: `refused` |
| Saída | Regra/versão/evidências registradas; sem fusão física, reatribuição de UUID ou autoativação nesta proposta |

A validação offline executada no helper para as fixtures positivas retornou: `11222333000181` → idêntico, `True`; `11.222.333/0001-81` → `11222333000181`, `True`; `12.ABC.345/01DE-35` → `12ABC34501DE35`, `True`. A fixture pré-existente `00ABC123000135` falhou DV (`False`) e foi descartada como positiva. Isso verifica somente fixtures/helper local, não cadastro oficial, situação atual, autenticidade, autoridade, unicidade ou aprovação da regra. Recibo bruto em `repair-1/commands/fixture-validation.txt`.



## Fonte vinculada e versionamento

A versão do normalizador e do validador deve vincular-se às implementações locais exatas `etl/src/bracc_etl/transforms/document_formatting.py::normalize_cnpj` e `::validate_cnpj`, incluindo commit e SHA-256 do código no bundle de execução. `bracc-cnpj-normalize-v1` e `bracc-cnpj-checkdigits-v1` são rótulos propostos, não identificadores/runtime já existentes; substituir por versões efetivas fixadas no momento da implementação aprovada. `etl/src/bracc_etl/identity/legacy.py::propose` usa `br:cnpj:establishment`, preserva UUIDs individuais e relata colisões; isso não implementa a regra proposta nem autoriza fusão.



| Caso | Entradas sintéticas | Resultado proposto | Razão |
|---|---|---|---|
| P1 | Dois `Company`, tupla/papel iguais, `11222333000181` e `11.222.333/0001-81`, evidência completa, períodos conhecidos compatíveis | `candidate` revisável, sem fusão | Fixture helper verificada |
| N1 | Dois `Company` com nome igual (“Comercial Aurora”), CNPJs distintos/ausentes | Não ligar; no máximo candidato por nome fora desta regra | Homônimo/nome não é identificador forte |
| C1 | Mesmo valor aparente, mas um lado é `PublicBody` e outro `Company` | Recusar ligação por conflito de papel | CNPJ compartilhado não torna papéis equivalentes |
| U1 | CNPJ parcial, mascarado ou ausente | `pending` (informação insuficiente; sem chave de ligação) | Não completar valor |
| U2 | CNPJ completo com formato inválido ou DV inválido | `refused` (não candidato; sem chave de ligação) | Não reparar identificador nem ignorar DV |
| T1 | Temporal obrigatório desconhecido ou precisão divergente | `pending` | Desconhecido não é positivo |
| T2 | Intervalos conhecidos, mesma precisão, sem interseção | `refused` | Incompatibilidade explícita |
| E1 | Evidência requerida ausente em qualquer ponta | `pending` | Proveniência insuficiente |
| A1 | `12.ABC.345/01DE-35` em ambas, papel/evidência e períodos compatíveis | `candidate` revisável; letras preservadas | Fixture helper verificada |

## Limites, riscos e exclusões

- Estrutura e DV corretos não comprovam que o CNPJ foi publicado pela fonte alegada, está ativo, pertence ao papel declarado ou representa a mesma entidade em qualquer período.
- Fusão por falso positivo pode contaminar relações, totais e projeções; falso negativo mantém duplicidades, preferível neste primeiro recorte fechado.
- Nome, similaridade, raiz CNPJ, CNPJ parcial, CPF, crosswalk inferido, chave de registro UUID e identificador interno do grafo não autorizam identidade global. UUID de registro identifica registro em namespace, não entidade.
- Excluídos: `Person`/CPF; contratos e seus espelhos; raiz/`LegalEntity`; sócios/QSA; órgãos/UGs; fundos/administradores; menções textuais; deduplicação por nome; validação de situação cadastral; qualquer alteração de dados reais, grafo, matcher ou automação.

## Requisitos de provenance e consumidor

Cada `EvidenceRef` de ambas as pontas deve amarrar o valor/campo e registro observados e trazer `source_id`, `dataset_id`, versão do contrato da fonte, UUID+SHA-256 do artefato, locator/tipo, `field_path`, identidade/chave do registro, valor observado e precisão/validade temporal. Identificar também versões de normalizador, validador e regra quando aplicáveis. Requisito ausente ou desconhecido: `pending`.

Reusar o `EvidenceRef` público definido pelo contrato consumidor U05; seus cinco achados seguem em reparo e ainda não foram aceitos. Mapear explicitamente os campos já presentes no DTO corrigido versus requisitos temporais/de versão deste desenho. Não duplicar DTO nem alterar U05 aqui. Campo ausente no DTO é extensão futura do produtor/contrato, sem transformar em pré-requisito especulativo; payload protege dados e não expõe identificador original por padrão.



## Saída e gates operacionais

A regra, se aprovada, só propõe candidato exato revisável; esta proposta não implementa resolução nem altera dados. A autorização contínua do usuário cobre engenharia M0 reversível (implementação, commits e PRs) depois da decisão de domínio favorável e dependências técnicas prontas, sem nova permissão por etapa. Revisão independente segue o fluxo. Escrita no grafo/dados reais, aquisição, aplicação de dados, merge, deploy, migração e operação exigem gates próprios explícitos. Issue #8 permanece aberta; sem alegação de encerramento amplo.
