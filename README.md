# PIX Platform

Plataforma de processamento de transações PIX, redesenhada para reduzir a latência percebida
pelo cliente (de ~4s P95 síncronos para uma resposta em dezenas de milissegundos) e para
escalar de forma independente entre ingestão e processamento. Contexto completo, decisões e
trade-offs em [`docs/decisions-and-tradeoffs.md`](docs/decisions-and-tradeoffs.md); diagramas
em [`docs/architecture.md`](docs/architecture.md).

## Stack

Java 21 · Spring Boot 3.3 · PostgreSQL (+ Flyway) · Apache Kafka (KRaft) · Resilience4j ·
Micrometer/Prometheus/Grafana · Testcontainers.

## Como rodar (Docker Compose — caminho principal)

Pré-requisito: Docker e Docker Compose.

```bash
docker compose up -d --build
```

Isso sobe: `postgres`, `kafka`, `pix-api` (porta `8080`), `pix-worker`, `prometheus`
(porta `9090`) e `grafana` (porta `3000`, login anônimo como admin).

`pix-api` e `pix-worker` são o mesmo artefato, diferenciados pela variável
`SPRING_PROFILES_ACTIVE` (`api` / `worker`) — ver [Papéis e escala](#papéis-e-escala).

### Testando o fluxo

```bash
# 1. Envia uma solicitação de PIX — responde imediatamente com 202 e status RECEIVED
curl -X POST localhost:8080/pix \
  -H 'Content-Type: application/json' \
  -d '{"transactionId":"tx-123456","amount":150.75,"pixKey":"cliente@email.com","description":"Pagamento de fatura"}'

# 2. Consulta o status (evolui para CONFIRMED ou FAILED em alguns segundos, em background)
curl localhost:8080/pix/tx-123456
```

Repetir o mesmo `POST` com o mesmo `transactionId` é seguro (idempotente) e retorna a
transação existente. Repetir com o mesmo `transactionId` mas dados diferentes retorna `409`.

### Forçando o cenário de falha / Dead Letter Topic

O parceiro é simulado com uma taxa de falha configurável (`app.partner.failure-rate`,
padrão `0.15`). Para observar o caminho de falha sustentada até o DLT:

```bash
docker compose stop pix-worker
docker compose run --rm -e SPRING_PROFILES_ACTIVE=worker -e DB_HOST=postgres \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e PARTNER_FAILURE_RATE=1.0 pix-worker
```

Envie uma transação nova; após esgotar os retries (Resilience4j + backoff do Kafka), o status
vira `FAILED_RETRYABLE` e a mensagem original pode ser vista em `pix.requested.dlt`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic pix.requested.dlt --from-beginning --max-messages 1
```

### Observabilidade

- Métricas da aplicação: `curl localhost:8080/actuator/prometheus`
- Prometheus (já com os alvos `pix-api`/`pix-worker` configurados): http://localhost:9090
- Grafana (datasource Prometheus já provisionado): http://localhost:3000 — monte painéis a
  partir de `pix_transactions_total`, `pix_processing_duration_seconds`,
  `pix_partner_call_duration_seconds`, `resilience4j_circuitbreaker_state`,
  `resilience4j_retry_calls_total` e as métricas de consumer lag do Kafka.
- Health: `curl localhost:8080/actuator/health`

### Escalando workers

```bash
docker compose up -d --scale pix-worker=3
```

## Papéis e escala

| Profile | O que sobe | Uso |
|---|---|---|
| `api` | REST (`POST`/`GET /pix`) + outbox relay | `pix-api` no compose |
| `worker` | Consumer Kafka + chamada ao parceiro + resiliência | `pix-worker` no compose |
| `all` | Ambos, no mesmo processo | conveniência para dev local / testes |

## Rodando localmente sem Docker (opcional)

Requer Java 21 e um Postgres + Kafka acessíveis (ou suba só essas duas dependências via
`docker compose up -d postgres kafka`).

```bash
export DB_HOST=localhost KAFKA_BOOTSTRAP_SERVERS=localhost:29092 SPRING_PROFILES_ACTIVE=all
mvn spring-boot:run
```

> Se o `java -version` da sua máquina não for 21 (`java -version` mostrando algo diferente
> de 21.x), aponte `JAVA_HOME` para um JDK 21 antes de rodar o Maven.

## Testes

```bash
mvn test
```

Inclui testes unitários (ingestão idempotente, orquestração do worker, mock do parceiro) e
testes de integração com Testcontainers (`*IT.java`) que sobem Postgres e Kafka reais e
exercitam o fluxo completo: happy path (`RECEIVED → CONFIRMED`), idempotência de POST
duplicado, 404 para transação inexistente, e o caminho de falha sustentada até
`FAILED_RETRYABLE` via DLT. Requer Docker disponível (Testcontainers sobe os containers).

## Configuração relevante

Todas com valor padrão em `application.yml`, sobrescrevíveis por variável de ambiente:

| Variável | Padrão | Descrição |
|---|---|---|
| `PARTNER_LATENCY_MS` | `2000` | latência simulada do parceiro |
| `PARTNER_FAILURE_RATE` | `0.15` | taxa de falha simulada (0–1) |
| `PARTNER_PERMANENT_REJECTION_SHARE` | `0.2` | fração das falhas que são rejeição definitiva (vs. transitória) |
| `KAFKA_PIX_REQUESTED_PARTITIONS` | `12` | partições do tópico principal |
| `OUTBOX_RELAY_FIXED_DELAY_MS` | `200` | intervalo do poller do outbox |
| `KAFKA_CONSUMER_MAX_ATTEMPTS` | `4` | tentativas do container Kafka antes do DLT |

## Endpoints

```
POST /pix
  {"transactionId": "tx-123456", "amount": 150.75, "pixKey": "cliente@email.com", "description": "..."}
  -> 202 Accepted (nova) | 200 OK (replay idempotente) | 409 Conflict (mesmo id, dados diferentes) | 400 Bad Request

GET /pix/{transactionId}
  -> 200 OK {"transactionId", "status", "amount", "pixKey", "description", "failureReason", "createdAt", "updatedAt"}
  -> 404 Not Found
```

`status` ∈ `RECEIVED`, `PROCESSING`, `CONFIRMED`, `FAILED`, `FAILED_RETRYABLE`.
