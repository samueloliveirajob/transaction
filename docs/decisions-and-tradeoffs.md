# Decisões técnicas e trade-offs

## 1. Como os gargalos foram identificados

A partir da descrição do fluxo atual (`API → Stored Procedure → Aplicação Legada → Core Transacional → Parceiro`) e do P95 de ~4s, os gargalos possíveis são:

1. **Latência acumulada em cadeia síncrona.** Cada salto espera o anterior terminar, incluindo a chamada ao parceiro (~2s, único ponto que o enunciado explicitamente fixa como imutável). Mesmo que todos os outros saltos fossem instantâneos, o cliente nunca ficaria abaixo de ~2s enquanto a chamada ao parceiro estiver no caminho síncrono da requisição.
2. **Ausência de amortecedor de carga.** Não há fila/buffer entre a API e o restante da cadeia — um pico de volume é entregue direto ao parceiro e ao banco, sem controle de backpressure.
3. **Lógica de negócio em stored procedure.** Difícil de testar, versionar e escalar horizontalmente; acopla o deploy de regra de negócio ao deploy de schema.
4. **Idempotência não mencionada no fluxo atual.** Um retry do cliente (por timeout, por exemplo) arrisca reprocessar a mesma transação.
5. **Sem padrão de resiliência explícito para a chamada ao parceiro** — uma falha transitória do parceiro provavelmente se propaga como erro definitivo ou prende uma thread/conexão pelo tempo total da chamada.

A decisão de arquitetura decorre diretamente do gargalo #1: **se o parceiro não pode ficar mais rápido, e continuará apresentando falhas temporárias, a única alavanca real é tirar essa espera do caminho síncrono do cliente.**

## 2. Decisões arquiteturais

### 2.1 Ingestão assíncrona com outbox transacional

`POST /pix` faz apenas uma transação local (inserir a transação + um evento de outbox) e responde `202 Accepted` com `status=PROCESSING`. Um relay (`OutboxRelay`, `@Scheduled`) publica os eventos não publicados no Kafka.

**Por quê outbox e não publicar direto no Kafka dentro do controller?** Publicar no Kafka e commitar no banco são duas operações que não compartilham uma transação. Se o commit no banco falhar depois do Kafka já ter recebido a mensagem (ou vice-versa), o sistema fica inconsistente — a transação existe sem processamento programado, ou é processada sem nunca ter sido persistida. O outbox escreve os dois fatos na mesma transação local; a publicação no Kafka é apenas *at-least-once* e pode ser refeita sem risco (idempotência cobre o restante).

**Trade-off assumido:** o relay é um poller simples (`FOR UPDATE SKIP LOCKED`, a cada 200ms por padrão), não CDC (ex: Debezium). Isso adiciona uma latência de publicação de até ~200ms e uma leitura extra no banco a cada tick — aceitável neste escopo. Um ambiente de produção de alto volume trocaria isso por CDC sem tocar em `PixIngestionService` nem no worker, já que ambos só dependem do contrato da tabela de outbox.

### 2.2 Kafka como broker, particionado por `transactionId`

Escolhido para demonstrar tratamento de sistemas distribuídos em escala: partições garantem ordenação por transação e permitem escalar o número de workers de forma independente da API. O consumer lag é o sinal de quando adicionar partições/workers — versus quando o limite real é a capacidade do próprio parceiro (não adianta ter mais workers do que o parceiro consegue absorver).

**Trade-off assumido:** Kafka traz complexidade operacional real (cluster, monitoramento, gestão de partições) comparado a alternativas mais simples como RabbitMQ ou SQS. Para o volume atual isso seria over-engineering; a escolha aqui é deliberada para atender ao pedido do enunciado de demonstrar conhecimento de sistemas distribuídos e preparar para crescimento significativo de volume. RabbitMQ seria uma escolha razoável para uma operação menor.

### 2.3 Idempotência em duas camadas

- **Na ingestão:** `transaction_id` tem constraint `UNIQUE` no banco. Um POST repetido com o mesmo `transactionId` e os mesmos dados retorna a transação existente (200, não cria linha nova). Se o mesmo `transactionId` vier com dados diferentes, é tratado como colisão de chave e retorna 409 — não como um retry seguro.
- **No worker:** antes de chamar o parceiro, o worker verifica se a transação já está em estado terminal (`CONFIRMED`, `FAILED`, `FAILED_RETRYABLE`) e, se estiver, ignora a mensagem. Isso protege contra reentrega do Kafka (que é *at-least-once* por padrão) causar uma segunda chamada ao parceiro para a mesma transação.

**Trade-off assumido:** optamos por *at-least-once* + consumidores idempotentes em vez de *exactly-once* do Kafka. Exactly-once do Kafka não resolveria o problema de qualquer forma, pois a chamada ao parceiro está fora de qualquer transação Kafka — a idempotência tem que existir na aplicação de qualquer jeito. Operar *at-least-once* é mais simples e barato.

### 2.4 Resiliência em duas janelas de tempo

- **Resilience4j** (`ResilientPartnerCaller`): `@Retry` + `@CircuitBreaker` + timeout manual (chamada isolada em um `ExecutorService` dedicado, com `Future.get(timeout)`), cobrindo falhas de milissegundos a poucos segundos dentro de uma única entrega de mensagem. O circuit breaker existe para que, quando o parceiro estiver degradado, as tentativas falhem rápido (`CallNotPermittedException`) em vez de cada uma pagar o timeout inteiro — é isso que evita que threads do worker se acumulem esperando um parceiro fora do ar.
- **Backoff do container Kafka** (`DefaultErrorHandler` + `FixedBackOff`): cobre falhas sustentadas que ultrapassam o orçamento de retry do Resilience4j, reentregando a mensagem inteira após alguns segundos.
- **Dead Letter Topic** (`pix.requested.dlt`): último recurso. Ao esgotar as tentativas do container, `ReconciliationDeadLetterRecoverer` marca a transação como `FAILED_RETRYABLE` (visível via `GET /pix/{id}`, em vez de ficar presa invisivelmente em `PROCESSING`) e publica a mensagem original no DLT para reprocessamento manual.

**Trade-off assumido:** duas janelas de retry (Resilience4j + Kafka) adicionam complexidade de configuração e podem, no pior caso, multiplicar tentativas efetivas contra o parceiro. Isso é intencional — cobre tanto uma falha de rede de meio segundo quanto uma indisponibilidade do parceiro de vários minutos, sem exigir que o cliente HTTP fique esperando nenhuma delas.

### 2.5 Remoção da stored procedure

A lógica de negócio passa a viver na aplicação (testável, versionável, deployável independentemente do schema). O Flyway assume a gestão de schema. Isso é uma perda deliberada de qualquer otimização/atomicidade que existisse dentro do banco, em troca de testabilidade e velocidade de entrega — e é pré-requisito para poder escalar a lógica horizontalmente, coisa que uma stored procedure não permite.

### 2.6 Papéis separados por profile Spring, mesmo artefato

`api`, `worker` e `all` (padrão local) controlam, via `@Profile`, quais beans sobem em cada instância. Em produção, `pix-api` e `pix-worker` são o mesmo JAR/imagem, escalados de forma independente (`docker compose up --scale pix-worker=N`). Isso evita manter dois repositórios/pipelines de build para um sistema que compartilha 90% do código de domínio.

## 3. Observabilidade e monitoramento

- **Logs estruturados** com `transactionId` no MDC (`Mdc.call`/`Mdc.run`), propagado desde a requisição HTTP e desde o consumo Kafka — permite correlacionar todo o ciclo de vida de uma transação nos logs da API e do worker.
- **Métricas de negócio** (`PixMetrics`, via Micrometer): contador `pix_transactions_total` por status, histograma `pix_processing_duration_seconds` (tempo do `RECEIVED` até um status terminal) e `pix_partner_call_duration_seconds` (latência efetiva da chamada ao parceiro, incluindo retries internos).
- **Métricas de resiliência** expostas automaticamente pelo Resilience4j (`resilience4j_circuitbreaker_state`, `resilience4j_retry_calls_total`, etc.) via `resilience4j-micrometer`.
- **Métricas de Kafka** (lag de consumer, taxa de produção/consumo) expostas automaticamente pela integração Spring Kafka + Micrometer — o lag do consumer é o principal sinal para decidir quando escalar workers.
- Tudo isso é exposto em `/actuator/prometheus` e coletado pelo Prometheus do `docker-compose.yml`; o Grafana já vem com o datasource provisionado (`docker/grafana/provisioning`), faltando apenas montar os painéis a partir das métricas acima.
- `/actuator/health` com probes (liveness/readiness) para orquestração (Kubernetes, por exemplo).

## 4. Falhas temporárias e recuperação

| Cenário | Comportamento |
|---|---|
| Falha transitória isolada do parceiro (ex: 1 timeout) | Resilience4j retry (com backoff exponencial + jitter) resolve dentro da mesma entrega de mensagem. |
| Degradação sustentada do parceiro | Circuit breaker abre; chamadas falham rápido; mensagens voltam para o backoff do Kafka em vez de acumular threads bloqueadas. |
| Indisponibilidade total, > orçamento de retry | Mensagem esgota as tentativas do container e vai para `pix.requested.dlt`; transação marcada `FAILED_RETRYABLE` (visível ao cliente/operação, não fica "presa"). |
| Crash do worker no meio do processamento | Kafka reentrega a mensagem a outro worker (rebalance de partição); o check de idempotência evita duplo processamento; se a transação já tinha sido confirmada mas o commit do offset falhou, a reentrega é um no-op seguro. |
| Réplica duplicada de `POST /pix` (mesmo `transactionId`) | Resolvido por constraint única + tratamento de `DataIntegrityViolationException`, sem duplicar linha nem reprocessar. |
| Falha ao publicar no outbox relay | A transação do relay faz rollback; as linhas voltam a ficar "não publicadas" e são reprocessadas no próximo tick — duplicata possível, mas inofensiva graças à idempotência do consumidor. |

## 5. Crescimento futuro

- **Escalar a API**: réplicas stateless atrás de um load balancer; cada requisição só faz uma transação local leve, então o P95 se mantém baixo mesmo sob alto RPS.
- **Escalar o worker**: aumentar partições de `pix.requested` e réplicas de `pix-worker` até o limite da capacidade real do parceiro (acompanhar consumer lag). Repartição é uma migração (requer planejamento), por isso o valor inicial (12) já considera alguma folga.
- **Banco**: se o volume de leitura de `GET /pix/{id}` crescer desproporcionalmente à escrita, adicionar réplica de leitura ou cache (ex: Redis) na frente da consulta de status — não implementado aqui por não ser o gargalo principal do enunciado, mas o ponto de extensão é único (`PixIngestionService.findByTransactionId`).
- **Outbox relay → CDC**: trocar o poller por Debezium quando o volume tornar o polling um gargalo perceptível de latência/carga no banco.
- **Novos consumidores**: `pix.processed` já existe como ponto de extensão (`NotificationConsumer` é um stub) para, por exemplo, notificar o cliente final via webhook, alimentar um pipeline de fraude, ou exportar para um ledger — sem tocar na ingestão ou no worker.
- **Multi-região / DR**: replicação do cluster Kafka (MirrorMaker 2 ou equivalente) e do Postgres seriam o próximo passo natural para tolerância a falhas de datacenter — fora do escopo deste teste.

## 6. Premissas assumidas

- A integração com o parceiro pode ser simulada por um mock configurável (`app.partner.*`), já que o enunciado permite simplificar/mocar integrações externas.
- O parceiro não expõe (ou não foi informado) um endpoint de consulta de status assíncrono — por isso a reconciliação de mensagens no DLT é tratada como um processo manual/operacional, não uma consulta automática ao parceiro. Se o parceiro tivesse tal endpoint, um job de reconciliação periódico seria a evolução natural.
- Um `transactionId` repetido com os mesmos dados é tratado como uma repetição seguro do mesmo pedido (idempotência); com dados diferentes, é tratado como erro do cliente (409), não como uma nova tentativa.
- Kafka com um único broker (KRaft, sem Zookeeper) é suficiente para demonstrar o desenho em ambiente local; um cluster de produção teria múltiplos brokers e fator de replicação > 1 (parametrizado via `app.kafka.replication-factor`).
- Não foi exigido frontend nem autenticação/autorização na API — a API está exposta sem camada de auth, o que não seria aceitável em produção para um endpoint financeiro real.
