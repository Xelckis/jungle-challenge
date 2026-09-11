package repository_test

import (
	"cmp"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"jungleChallenge/internal/domain"
	"jungleChallenge/internal/repository"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	ctx := t.Context()
	dbURL := cmp.Or(os.Getenv("DATABASE_URL"), "postgres://jungle_user:jungle_password@127.0.0.1:5432/jungle_db?sslmode=disable")
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("Ignorando teste de integracao com banco real: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("PostgreSQL nao disponivel para testes de integracao: %v", err)
	}
	return pool
}

func TestWalletCreationAndOpening(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	repo := repository.NewWalletRepository(pool)
	ctx := t.Context()

	t.Run("Criacao de carteira com saldo positivo gera OPENING e ledger", func(t *testing.T) {
		playerID := "player-opening-" + uuid.New().String()
		walletID := uuid.New().String()
		initMoney, _ := domain.NewMoney("500.00", "BRL")
		wallet := domain.NewWallet(walletID, playerID, initMoney)

		err := repo.Create(ctx, wallet)
		require.NoError(t, err)

		saved, err := repo.GetWalletByID(ctx, walletID)
		require.NoError(t, err)
		assert.Equal(t, "500.00", saved.Balance.String())
		assert.Equal(t, 1, saved.Version)

		ledger, err := repo.GetLedger(ctx, walletID, time.Now().UTC().Add(time.Hour), 10)
		require.NoError(t, err)

		require.Len(t, ledger, 1)
		assert.Equal(t, "CREDIT", ledger[0].Direction)

		amount, _ := domain.NewMoney(ledger[0].Amount, "BRL")
		balanceBefore, _ := domain.NewMoney(ledger[0].BalanceBefore, "BRL")
		balanceAfter, _ := domain.NewMoney(ledger[0].BalanceAfter, "BRL")

		assert.Equal(t, "500.00", amount.String())
		assert.Equal(t, "0.00", balanceBefore.String())
		assert.Equal(t, "500.00", balanceAfter.String())

	})

	t.Run("Criacao de carteira com saldo zero nao gera ledger nem OPENING", func(t *testing.T) {
		playerID := "player-zero-" + uuid.New().String()
		walletID := uuid.New().String()
		zeroMoney, _ := domain.NewZeroMoney("BRL")
		wallet := domain.NewWallet(walletID, playerID, zeroMoney)

		err := repo.Create(ctx, wallet)
		require.NoError(t, err)

		ledger, err := repo.GetLedger(ctx, walletID, time.Now().UTC().Add(time.Hour), 10)
		require.NoError(t, err)
		assert.Empty(t, ledger)
	})
}

func TestWagerOperations_AllKinds(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	repo := repository.NewWalletRepository(pool)
	ctx := t.Context()

	playerID := "player-ops-" + uuid.New().String()
	walletID := uuid.New().String()
	initMoney, _ := domain.NewMoney("100.00", "BRL")
	wallet := domain.NewWallet(walletID, playerID, initMoney)
	require.NoError(t, repo.Create(ctx, wallet))

	t.Run("BET debita saldo da carteira", func(t *testing.T) {
		betMoney, _ := domain.NewMoney("30.00", "BRL")
		wager := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindBet, betMoney)
		wager.ProviderID = "provider-test"
		wager.ExternalTransactionID = "bet-" + uuid.New().String()
		wager.PlayerID = playerID
		idempKey := "key-" + wager.ExternalTransactionID
		hash := domain.ComputePayloadHash(wager.ProviderID, wager.ExternalTransactionID, playerID, walletID, "", "", "BET", betMoney, "")

		res, err := repo.ProcessTransaction(ctx, wager, idempKey, hash, "", "")
		require.NoError(t, err)
		assert.Equal(t, domain.StatusProcessed, res.Status)
		assert.Equal(t, "70.00", res.Balance.String())
	})

	t.Run("WIN credita saldo da carteira", func(t *testing.T) {
		winMoney, _ := domain.NewMoney("50.00", "BRL")
		wager := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindWin, winMoney)
		wager.ProviderID = "provider-test"
		wager.ExternalTransactionID = "win-" + uuid.New().String()
		wager.PlayerID = playerID
		idempKey := "key-" + wager.ExternalTransactionID
		hash := domain.ComputePayloadHash(wager.ProviderID, wager.ExternalTransactionID, playerID, walletID, "", "", "WIN", winMoney, "")

		res, err := repo.ProcessTransaction(ctx, wager, idempKey, hash, "", "")
		require.NoError(t, err)
		assert.Equal(t, domain.StatusProcessed, res.Status)
		assert.Equal(t, "120.00", res.Balance.String())
	})

	t.Run("LOSS nao altera saldo nem cria ledger", func(t *testing.T) {
		lossMoney, _ := domain.NewZeroMoney("BRL")
		wager := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindLoss, lossMoney)
		wager.ProviderID = "provider-test"
		wager.ExternalTransactionID = "loss-" + uuid.New().String()
		wager.PlayerID = playerID
		idempKey := "key-" + wager.ExternalTransactionID
		hash := domain.ComputePayloadHash(wager.ProviderID, wager.ExternalTransactionID, playerID, walletID, "", "", "LOSS", lossMoney, "")

		res, err := repo.ProcessTransaction(ctx, wager, idempKey, hash, "", "")
		require.NoError(t, err)
		assert.Equal(t, domain.StatusProcessed, res.Status)
		assert.Equal(t, "120.00", res.Balance.String())
	})

	t.Run("BET com saldo insuficiente e rejeitada de forma persistente", func(t *testing.T) {
		largeMoney, _ := domain.NewMoney("200.00", "BRL")
		wager := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindBet, largeMoney)
		wager.ProviderID = "provider-test"
		wager.ExternalTransactionID = "bet-fail-" + uuid.New().String()
		wager.PlayerID = playerID
		idempKey := "key-" + wager.ExternalTransactionID
		hash := domain.ComputePayloadHash(wager.ProviderID, wager.ExternalTransactionID, playerID, walletID, "", "", "BET", largeMoney, "")

		res, err := repo.ProcessTransaction(ctx, wager, idempKey, hash, "", "")
		require.NoError(t, err)
		assert.Equal(t, domain.StatusRejected, res.Status)
		assert.Equal(t, domain.FailureCodeInsufficientFunds, res.FailureCode)
		assert.Equal(t, "120.00", res.Balance.String())

		// Replay da mesma aposta rejeitada devolve status REJECTED
		replayRes, err := repo.ProcessTransaction(ctx, wager, idempKey, hash, "", "")
		require.NoError(t, err)
		assert.Equal(t, domain.StatusRejected, replayRes.Status)
		assert.True(t, replayRes.IdempotentReplay)
	})
}

func TestIdempotencyAndConflicts(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	repo := repository.NewWalletRepository(pool)
	ctx := t.Context()

	playerID := "player-idemp-" + uuid.New().String()
	walletID := uuid.New().String()
	initMoney, _ := domain.NewMoney("100.00", "BRL")
	wallet := domain.NewWallet(walletID, playerID, initMoney)
	require.NoError(t, repo.Create(ctx, wallet))

	betMoney, _ := domain.NewMoney("25.00", "BRL")
	wager := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindBet, betMoney)
	wager.ProviderID = "provider-a"
	wager.ExternalTransactionID = "tx-123-" + uuid.New().String()
	wager.PlayerID = playerID
	idempKey := "idemp-" + wager.ExternalTransactionID
	hash1 := domain.ComputePayloadHash(wager.ProviderID, wager.ExternalTransactionID, playerID, walletID, "", "", "BET", betMoney, "")

	// 1. Processamento original
	res1, err := repo.ProcessTransaction(ctx, wager, idempKey, hash1, "", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessed, res1.Status)
	assert.False(t, res1.IdempotentReplay)

	// 2. Replay idempotente com mesma chave e mesmo hash
	res2, err := repo.ProcessTransaction(ctx, wager, idempKey, hash1, "", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessed, res2.Status)
	assert.True(t, res2.IdempotentReplay)
	assert.Equal(t, res1.TransactionID, res2.TransactionID)
	assert.Equal(t, res1.Balance.String(), res2.Balance.String())

	// 3. Conflito de idempotencia: mesma chave com hash diferente
	hashDiff := domain.ComputePayloadHash(wager.ProviderID, wager.ExternalTransactionID, playerID, walletID, "diff-round", "", "BET", betMoney, "")
	_, err = repo.ProcessTransaction(ctx, wager, idempKey, hashDiff, "", "")
	assert.ErrorIs(t, err, domain.ErrIdempotencyConflict)

	// 4. Reutilizacao de external tx ID com chave diferente
	diffKey := "other-key-" + uuid.New().String()
	_, err = repo.ProcessTransaction(ctx, wager, diffKey, hash1, "", "")
	assert.ErrorIs(t, err, domain.ErrExternalKeyReused)
}

func TestConcurrentBets_RaceBetweenTwoBets(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	repo := repository.NewWalletRepository(pool)
	ctx := t.Context()

	// Cenário obrigatório: Carteira com 100.00 recebe 2 apostas concorrentes de 80.00
	playerID := "player-race-" + uuid.New().String()
	walletID := uuid.New().String()
	initMoney, _ := domain.NewMoney("100.00", "BRL")
	wallet := domain.NewWallet(walletID, playerID, initMoney)
	require.NoError(t, repo.Create(ctx, wallet))

	betMoney, _ := domain.NewMoney("80.00", "BRL")

	var wg sync.WaitGroup
	results := make(chan *domain.ProcessResult, 2)

	for range 2 {
		wg.Go(func() {
			w := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindBet, betMoney)
			w.ProviderID = "provider-race"
			w.ExternalTransactionID = uuid.New().String()
			w.PlayerID = playerID
			idempKey := "key-" + w.ExternalTransactionID
			hash := domain.ComputePayloadHash(w.ProviderID, w.ExternalTransactionID, playerID, walletID, "", "", "BET", betMoney, "")

			res, _ := repo.ProcessTransaction(context.Background(), w, idempKey, hash, "", "")
			results <- res
		})
	}

	wg.Wait()
	close(results)

	var processedCount, rejectedCount int
	for res := range results {
		if res != nil {
			if res.Status == domain.StatusProcessed {
				processedCount++
			} else if res.Status == domain.StatusRejected {
				rejectedCount++
			}
		}
	}

	assert.Equal(t, 1, processedCount, "Exatamente 1 aposta deve ser processada")
	assert.Equal(t, 1, rejectedCount, "Exatamente 1 aposta deve ser rejeitada por saldo insuficiente")

	finalWallet, err := repo.GetWalletByID(ctx, walletID)
	require.NoError(t, err)
	assert.Equal(t, "20.00", finalWallet.Balance.String(), "Saldo final deve ser exatamente 20.00 BRL")
}

func TestReversals_OutOfOrderAndReferenceWorker(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	repo := repository.NewWalletRepository(pool)
	ctx := t.Context()

	playerID := "player-rev-" + uuid.New().String()
	walletID := uuid.New().String()
	initMoney, _ := domain.NewMoney("100.00", "BRL")
	wallet := domain.NewWallet(walletID, playerID, initMoney)
	require.NoError(t, repo.Create(ctx, wallet))

	betExtID := "bet-ref-" + uuid.New().String()
	betMoney, _ := domain.NewMoney("40.00", "BRL")

	// 1. REFUND chega antes da BET original
	refundMoney, _ := domain.NewMoney("40.00", "BRL")
	refundTx := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindRefund, refundMoney)
	refundTx.ProviderID = "provider-ref-test"
	refundTx.ExternalTransactionID = "refund-" + uuid.New().String()
	refundTx.ReferenceExternalTxID = betExtID
	refundTx.PlayerID = playerID
	refundIdempKey := "key-" + refundTx.ExternalTransactionID
	refundHash := domain.ComputePayloadHash(refundTx.ProviderID, refundTx.ExternalTransactionID, playerID, walletID, "", "", "REFUND", refundMoney, betExtID)

	refResult, err := repo.ProcessTransaction(ctx, refundTx, refundIdempKey, refundHash, "", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPendingReference, refResult.Status, "REFUND antes da BET deve ficar PENDING_REFERENCE")

	// 2. Tenta resolver antes da BET existir -> continua pendente
	found, err := repo.ResolveNextPendingReference(ctx, 24*time.Hour)
	require.NoError(t, err)
	assert.True(t, found)

	// 3. BET original chega e e processada
	betTx := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindBet, betMoney)
	betTx.ProviderID = "provider-ref-test"
	betTx.ExternalTransactionID = betExtID
	betTx.PlayerID = playerID
	betIdempKey := "key-" + betExtID
	betHash := domain.ComputePayloadHash(betTx.ProviderID, betTx.ExternalTransactionID, playerID, walletID, "", "", "BET", betMoney, "")

	betResult, err := repo.ProcessTransaction(ctx, betTx, betIdempKey, betHash, "", "")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessed, betResult.Status)
	assert.Equal(t, "60.00", betResult.Balance.String())

	// 4. Reference worker resolve a pendencia do REFUND
	foundResolved, err := repo.ResolveNextPendingReference(ctx, 24*time.Hour)
	require.NoError(t, err)
	assert.True(t, foundResolved)

	// Saldo deve voltar para 100.00 (60 + 40)
	finalWallet, err := repo.GetWalletByID(ctx, walletID)
	require.NoError(t, err)
	assert.Equal(t, "100.00", finalWallet.Balance.String())
}

func TestReconciliation(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	repo := repository.NewWalletRepository(pool)
	ctx := t.Context()

	playerID := "player-rec-" + uuid.New().String()
	walletID := uuid.New().String()
	initMoney, _ := domain.NewMoney("1000.00", "BRL")
	wallet := domain.NewWallet(walletID, playerID, initMoney)
	require.NoError(t, repo.Create(ctx, wallet))

	betMoney, _ := domain.NewMoney("25.00", "BRL")
	wager := domain.NewWagerTransaction(uuid.New().String(), walletID, domain.KindBet, betMoney)
	wager.ProviderID = "provider-rec"
	wager.ExternalTransactionID = uuid.New().String()
	wager.PlayerID = playerID
	idempKey := "key-" + wager.ExternalTransactionID
	hash := domain.ComputePayloadHash(wager.ProviderID, wager.ExternalTransactionID, playerID, walletID, "", "", "BET", betMoney, "")

	_, err := repo.ProcessTransaction(ctx, wager, idempKey, hash, "", "")
	require.NoError(t, err)

	report, err := repo.Reconcile(ctx, walletID)
	require.NoError(t, err)
	assert.Equal(t, walletID, report.WalletID)
	assert.Equal(t, "975.00", report.StoredBalance)
	assert.Equal(t, "975.00", report.CalculatedBalance)
	assert.Equal(t, "0.00", report.Difference)
	assert.True(t, report.Consistent)
	assert.Equal(t, 2, report.CheckedEntries) // 1 opening + 1 bet
}
