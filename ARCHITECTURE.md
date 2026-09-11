# Arquitetura do Sistema — Processamento Distribuído de Apostas

Este documento detalha as decisões técnicas, garantias financeiras, padrões arquiteturais e mecanismos de resiliência implementados no serviço de processamento de apostas.

---

## 1. Representação Monetária (Money)

- **Tipo e Precisão**: Valores monetários são modelados através do Value Object imutável `domain.Money`, utilizando a biblioteca `github.com/shopspring/decimal` com precisão decimal exata.
- **Proibição de Ponto Flutuante**: Nenhuma operação (parsing, cálculo, serialização JSON ou persistência SQL) utiliza `float32` ou `float64`.
- **Validações e Regras**:
  - Escala fixa de no máximo duas casas decimais (`amount.Exponent() >= -2`).
  - Código de moeda ISO 4217 com 3 letras maiúsculas.
  - Rejeição estrita de notação científica (`e`/`E`), `NaN`, `Inf`, strings vazias e valores negativos em entradas financeiras externas.
  - Aritmética e comparações exigem moedas idênticas (`ErrInvalidCurrency`).
- **Persistência**: Mapeado no PostgreSQL como tipo `NUMERIC(15, 2)` com constraints de não negatividade (`CHECK (balance >= 0)` e `CHECK (amount >= 0)`).

---

## 2. Controle Transacional e Persistência

- **Biblioteca**: Utiliza `pgx/v5` (`pgxpool.Pool`) com queries SQL nativas e explícitas.
- **Nível de Isolamento**: Todas as operações de escrita utilizam transações com nível de isolamento `ReadCommitted`.
- **Delimitação de Transação**:
  - A atomicidade engloba na mesma transação SQL:
    1. Registro da mensagem na `inbox` (se recebida via SQS).
    2. Bloqueio pessimista da carteira (`SELECT ... FOR UPDATE`).
    3. Validação de invariantes de negócio e verificação de idempotência.
    4. Inserção na tabela `wager_transactions` (com status `PROCESSED`, `PENDING_REFERENCE` ou `REJECTED`).
    5. Inserção no `wallet_ledger` (imutável e append-only).
    6. Atualização de saldo e versão da carteira (`wallets`).
    7. Inserção de eventos na tabela `outbox`.
  - O uso de `defer tx.Rollback(ctx)` garante rollback automático e seguro caso ocorra qualquer erro intermediário antes do `tx.Commit(ctx)`.

---

## 3. Concorrência e Locking

- **Estratégia**: Locking pessimista a nível de linha no PostgreSQL (`SELECT ... FOR UPDATE` pelo `wallet_id`).
- **Coordenação por Carteira**: O lock bloqueia exclusivamente a carteira em processamento. Carteiras distintas são processadas concorrentemente em paralelo, eliminando contenções e proibindo locks globais.
- **Disputa de Saldo (Exemplo Obrigatório)**:
  - Duas apostas simultâneas de `80.00 BRL` sobre uma carteira de `100.00 BRL`:
    - A primeira transação adquire o lock, debita `80.00 BRL`, gera lançamento no ledger, grava o evento de outbox e confirma saldo de `20.00 BRL`.
    - A segunda transação adquire o lock após o commit da primeira, detecta saldo insuficiente (`20.00 < 80.00`), persiste a transação como `REJECTED` (com `failure_code = 'INSUFFICIENT_FUNDS'`), emite o evento `WagerTransactionRejected` na outbox e faz commit sem alterar o saldo ou criar ledger.
    - Reenvios subsequentes de qualquer das apostas identificam a chave persistida e retornam o estado original idempotente.

---

## 4. Idempotência Persistente e Hash Canônico

- **Chave de Idempotência**: O header `Idempotency-Key` é obrigatório nos endpoints HTTP e preservado no envelope de mensagens SQS.
- **Hash Canônico Determinístico**:
  - Calculado via `SHA-256` sobre uma struct canônica contendo estritamente os campos de negócio ordenados: `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `moneyAmount` (2 casas decimais), `moneyCurrency` e `referenceExternalTransactionId`.
  - Exclui metadados de transporte e timestamps efêmeros.
- **Comportamentos**:
  - **Replay Idempotente**: Mesma chave e mesmo hash canônico -> Retorna o resultado persistido com `idempotentReplay: true` e o saldo registrado.
  - **Conflito de Idempotência**: Mesma chave com payload/hash diferente -> Retorna `HTTP 409 Conflict` (`ErrIdempotencyConflict`).
  - **Reutilização de Identificador Externo**: Mesmo `(providerId, externalTransactionId)` com chave diferente -> Retorna `HTTP 409 Conflict` (`ErrExternalKeyReused`).

---

## 5. Reversões e Transações Pendentes (Reference Worker)

- **Assincronia Distribuída**: Reversões (`REFUND` ou `ROLLBACK`) que chegam antes da aposta original são salvas com status `PENDING_REFERENCE` e emitem `WagerTransactionPendingReference` na outbox.
- **ReferenceWorker**:
  - Executa polling periódico com `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` em transações `PENDING_REFERENCE`.
  - **TTL (24h)**: Se o tempo decorrido exceder o TTL, a transação é finalizada como `REJECTED` (`failure_code = 'REFERENCE_TIMEOUT'`) e emite `WagerTransactionRejected`.
  - **Resolução**: Quando a transação referenciada é confirmada como `PROCESSED`:
    - Valida compatibilidade de valor, provedor, carteira e moeda.
    - Previne devoluções duplicadas (verifica se já houve `REFUND` ou `ROLLBACK` prévio).
    - Aplica o movimento financeiro correspondente (crédito para `REFUND` e `ROLLBACK` de `BET`; débito para `ROLLBACK` de `WIN`/`REFUND`).
    - Atualiza a transação para `PROCESSED` e emite `WalletBalanceChanged` e `WagerTransactionProcessed`.

---

## 6. Padrões Inbox e Transactional Outbox

- **Transactional Outbox**:
  - Nenhum evento externo é enviado antes da confirmação do commit da transação.
  - O `OutboxWorker` realiza polling contínuo com `FOR UPDATE SKIP LOCKED` e publica mensagens nas filas FIFO do SQS utilizando:
    - `MessageGroupId`: `aggregate_id` (ID da carteira, garantindo ordenação FIFO por carteira).
    - `MessageDeduplicationId`: `eventId` estável do envelope.
  - Após publicação bem-sucedida, marca `published = TRUE`.
- **Inbox Pattern**:
  - Registros de mensagens SQS na tabela `inbox` com constraint única `(consumer_name, message_id)`.
  - Garantia de entrega *at-least-once* com processamento *exactly-once* efetivo no banco.

---

## 7. Autenticação, Autorização e Isolamento

- **IdP Externo**: Integrado com **Keycloak** via OAuth 2.0 / OIDC.
- **Fluxo de Serviço**: `client_credentials` utilizado pelos provedores de jogos.
- **Middleware `AuthMiddleware`**:
  - Valida o token JWT via JWKS do Keycloak (`go-oidc`).
  - Extrai a claim `clientId` / `azp` e injeta o `providerId` no contexto da requisição.
- **Isolamento Estrito**:
  - Provedores só podem processar e consultar transações cujo `providerId` corresponda à identidade autenticada no token (`HTTP 403 Forbidden` em caso de divergência).
  - Endpoints de gestão de carteira (`/wallets...`) são de uso exclusivo do serviço interno.

---

## 8. Composição com Uber Fx e Shutdown Gracioso

- **Injeção de Dependências**: Todas as dependências (logger Zap, pool pgx, cliente SQS, repositórios, handlers, middleware e workers) são instanciadas via `fx.Provide` e inicializadas via `fx.Invoke`.
- **Ciclo de Vida (`fx.Lifecycle`)**:
  - `OnStart`: Valida conexões, inicializa o servidor HTTP e inicia as goroutines dos workers (`OutboxWorker`, `InboxWorker`, `ReferenceWorker`).
  - `OnStop`:
    1. Interrompe a entrada de novas requisições e faz o `Shutdown` gracioso do servidor HTTP.
    2. Envia sinal de encerramento (`close(done)`) para os workers, aguardando término do trabalho em andamento.
    3. Fecha o pool de conexões com o PostgreSQL (`pool.Close()`).
