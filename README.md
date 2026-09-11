# Desafio Backend — Processamento Distribuído de Apostas em Go

Serviço de core financeiro para processamento de transações de apostas e gestão de carteiras de jogadores em ambiente distribuído, construído com **Go**, **Uber Fx**, **PostgreSQL (pgx)**, **AWS SQS (LocalStack)** e **Keycloak (OAuth 2.0/OIDC)**.

---

## 1. Pré-requisitos

- **Go**: 1.22+ (ou Go 1.24+ / Go 1.27)
- **Docker** e **Docker Compose**
- **Git**

---

## 2. Configuração do Ambiente

Copie o arquivo de exemplo de variáveis de ambiente:

```bash
cp .env.example .env
```

### Inicializando a Infraestrutura Local

O Docker Compose sobe automaticamente os serviços:
1. **PostgreSQL** (`jungle_postgres` na porta `5432` com schema inicial aplicado)
2. **LocalStack** (`jungle_localstack` na porta `4566` com as filas FIFO criadas)
3. **Keycloak** (`jungle_keycloak` na porta `8080` com o realm `jungle-realm` provisionado e clientes de teste)

```bash
docker compose up -d
```

### Inicialização e Verificação das Filas SQS

As filas FIFO são criadas automaticamente pelo script `localstack-init.sh` na inicialização do container LocalStack:
- `wager-transactions.fifo`
- `wager-transactions-dlq.fifo` (com Redrive Policy e `maxReceiveCount=3`)

---

## 3. Executando a Aplicação

Para compilar e iniciar a aplicação:

```bash
go run main.go
```

A aplicação iniciará na porta `8081` gerenciada pelo **Uber Fx**, conectando-se ao PostgreSQL, SQS e Keycloak, e disparando os workers em segundo plano:
- `OutboxWorker` (leitura de outbox e publicação no SQS com `FOR UPDATE SKIP LOCKED`)
- `InboxWorker` (consumo durável com deduplicação no banco)
- `ReferenceWorker` (resolução contínua de transações `PENDING_REFERENCE`)

---

## 4. Migrations

As migrations estão localizadas em `migrations/000001_init_schema.up.sql` e `migrations/000001_init_schema.down.sql`.

- **Aplicação Automática**: A aplicação executa e valida o schema na inicialização (`//go:embed`).
- **Aplicação via Makefile**:
  ```bash
  make migrate-up
  ```
- **Reversão via Makefile**:
  ```bash
  make migrate-down
  ```

---

## 5. Autenticação e Provedores (Keycloak)

O Keycloak é pré-configurado com o realm `jungle-realm` e clientes com fluxo `client_credentials`:
- Client ID: `provider-a` (Secret: `secret-a` ou configurado no realm)
- Client ID: `provider-b`

### Obtendo Token de Acesso (Client Credentials)

```bash
curl -X POST "http://localhost:8080/realms/jungle-realm/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=provider-a" \
  -d "client_secret=secret-a"
```

---

## 6. Exemplos de Chamadas da API HTTP

### 6.1. Abertura Interna de Carteira (POST /wallets)

```bash
curl -X POST "http://localhost:8081/wallets" \
  -H "Content-Type: application/json" \
  -d '{
    "playerId": "player-001",
    "initialBalance": {
      "amount": "1000.00",
      "currency": "BRL"
    }
  }'
```

Resposta (201 Created):
```json
{
  "id": "c1f7b80a-9d8c-4f51-b847-c3765103a812",
  "playerId": "player-001",
  "balance": {
    "amount": "1000.00",
    "currency": "BRL"
  },
  "version": 1
}
```

### 6.2. Envio de Operação de Aposta (POST /wagering/transactions)

Endpoint protegido por token OIDC. O `providerId` da requisição deve corresponder ao `clientId` autenticado no token:

```bash
curl -X POST "http://localhost:8081/wagering/transactions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <SEU_TOKEN_JWT>" \
  -H "Idempotency-Key: provider-a:tx-1001" \
  -H "X-Correlation-ID: req-001" \
  -d '{
    "providerId": "provider-a",
    "externalTransactionId": "tx-1001",
    "playerId": "player-001",
    "walletId": "c1f7b80a-9d8c-4f51-b847-c3765103a812",
    "roundId": "round-99",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": {
      "amount": "25.00",
      "currency": "BRL"
    }
  }'
```

Resposta (200 OK):
```json
{
  "transactionId": "0192f298-345e-7e38-af88-e43f851a819d",
  "status": "PROCESSED",
  "balance": {
    "amount": "975.00",
    "currency": "BRL"
  },
  "idempotentReplay": false
}
```

### 6.3. Consulta de Transação Externa (GET com Isolamento)

```bash
curl -X GET "http://localhost:8081/providers/provider-a/wagering/transactions/tx-1001" \
  -H "Authorization: Bearer <SEU_TOKEN_JWT>"
```

### 6.4. Reconciliação Financeira (POST /wallets/:walletId/reconciliation)

```bash
curl -X POST "http://localhost:8081/wallets/c1f7b80a-9d8c-4f51-b847-c3765103a812/reconciliation"
```

Resposta:
```json
{
  "walletId": "c1f7b80a-9d8c-4f51-b847-c3765103a812",
  "storedBalance": {
    "amount": "975.00",
    "currency": "BRL"
  },
  "calculatedBalance": {
    "amount": "975.00",
    "currency": "BRL"
  },
  "difference": {
    "amount": "0.00",
    "currency": "BRL"
  },
  "consistent": true,
  "checkedEntries": 2
}
```

### 6.5. Health Checks Públicos

- `GET /health/live` (Liveness do processo)
- `GET /health/ready` (Readiness de PostgreSQL e SQS)

---

## 7. Execução de Testes

Para executar toda a suíte de testes:

```bash
go test -v ./...
```

Para executar testes com detecção de concorrência / corrida de dados:

```bash
go test -v -race ./...
```

Para checagem estática de código:

```bash
go vet ./...
```
