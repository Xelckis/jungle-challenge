package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"jungleChallenge/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type WalletRepository struct {
	db *pgxpool.Pool
}

func NewWalletRepository(db *pgxpool.Pool) *WalletRepository {
	return &WalletRepository{db: db}
}

func (r *WalletRepository) Create(ctx context.Context, w domain.Wallet) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("falha ao iniciar transacao: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err = tx.Exec(ctx, query,
		w.ID,
		w.PlayerID,
		w.Balance.Currency,
		w.Balance.Amount.StringFixed(2),
		w.Version,
		w.CreatedAt,
		w.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("falha ao inserir carteira: %w", err)
	}

	if w.Balance.Amount.IsPositive() {
		openingTx := domain.WagerTransaction{
			ID:                    uuid.New().String(),
			WalletID:              w.ID,
			ProviderID:            "INTERNAL",
			ExternalTransactionID: "OPENING-" + w.ID,
			PlayerID:              w.PlayerID,
			Kind:                  domain.KindOpening,
			Money:                 w.Balance,
			Status:                domain.StatusProcessed,
			CreatedAt:             w.CreatedAt,
			FinishedAt:            &w.CreatedAt,
		}

		payloadHashBytes := sha256.Sum256([]byte(openingTx.ExternalTransactionID))
		payloadHash := hex.EncodeToString(payloadHashBytes[:])

		_, err = tx.Exec(ctx, `
			INSERT INTO wager_transactions 
			(id, wallet_id, provider_id, external_transaction_id, idempotency_key, payload_hash, player_id, kind, status, amount, currency, created_at, finished_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
			openingTx.ID, openingTx.WalletID, openingTx.ProviderID, openingTx.ExternalTransactionID, openingTx.ExternalTransactionID,
			payloadHash, openingTx.PlayerID, string(openingTx.Kind), string(openingTx.Status),
			openingTx.Money.Amount.StringFixed(2), openingTx.Money.Currency, openingTx.CreatedAt, openingTx.FinishedAt)
		if err != nil {
			return fmt.Errorf("falha ao inserir transacao de abertura: %w", err)
		}

		ledgerID := uuid.New().String()
		_, err = tx.Exec(ctx, `
			INSERT INTO wallet_ledger (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			ledgerID, w.ID, openingTx.ID, "CREDIT", w.Balance.Amount.StringFixed(2), "0.00", w.Balance.Amount.StringFixed(2), w.CreatedAt)
		if err != nil {
			return fmt.Errorf("falha ao inserir ledger de abertura: %w", err)
		}

		if err := r.insertOutboxProcessed(ctx, tx, openingTx, ""); err != nil {
			return fmt.Errorf("falha ao inserir outbox WagerTransactionProcessed: %w", err)
		}

		if err := r.insertOutboxBalanceChanged(ctx, tx, w.ID, openingTx.ID, "CREDIT", w.Balance, "0.00", w.Balance.Amount.StringFixed(2), w.Version, ""); err != nil {
			return fmt.Errorf("falha ao inserir outbox WalletBalanceChanged: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("falha no commit da carteira: %w", err)
	}

	return nil
}

func (r *WalletRepository) ProcessTransaction(ctx context.Context, wager domain.WagerTransaction, idempotencyKey, payloadHash, messageID string, correlationID string) (*domain.ProcessResult, error) {
	if wager.Kind == domain.KindOpening {
		return nil, errors.New("tipo OPENING e reservado para criacao interna de carteira")
	}

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("falha ao iniciar transacao: %w", err)
	}
	defer tx.Rollback(ctx)

	// Se veio via Inbox (SQS), registrar na tabela inbox
	if messageID != "" {
		_, err = tx.Exec(ctx, "INSERT INTO inbox (message_id, consumer_name, payload_hash) VALUES ($1, $2, $3) ON CONFLICT (consumer_name, message_id) DO NOTHING", messageID, "sqs-wager-consumer", payloadHash)
		if err != nil {
			return nil, fmt.Errorf("falha ao registrar na inbox: %w", err)
		}
	}

	// 1. Checagem de Idempotência: mesma chave
	var existingID, existingStatus, existingHash, existingFailureCode, existingWalletID string
	err = tx.QueryRow(ctx, "SELECT id, status, payload_hash, COALESCE(failure_code, ''), wallet_id FROM wager_transactions WHERE idempotency_key = $1", idempotencyKey).
		Scan(&existingID, &existingStatus, &existingHash, &existingFailureCode, &existingWalletID)

	if err == nil {
		if existingHash != payloadHash {
			return nil, domain.ErrIdempotencyConflict
		}

		// Replay Idempotente
		var balance domain.Money
		var balStr, currStr string
		errBal := tx.QueryRow(ctx, "SELECT balance, currency FROM wallets WHERE id = $1", existingWalletID).Scan(&balStr, &currStr)
		if errBal == nil {
			balance, _ = domain.NewMoney(balStr, currStr)
		}

		if existingStatus == string(domain.StatusProcessed) {
			var ledgerBalAfter string
			errLedger := tx.QueryRow(ctx, "SELECT balance_after FROM wallet_ledger WHERE transaction_id = $1", existingID).Scan(&ledgerBalAfter)
			if errLedger == nil {
				balance, _ = domain.NewMoney(ledgerBalAfter, currStr)
			}
		}

		return &domain.ProcessResult{
			TransactionID:    existingID,
			Status:           domain.TxStatus(existingStatus),
			Balance:          balance,
			IdempotentReplay: true,
			FailureCode:      existingFailureCode,
		}, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("erro ao consultar chave de idempotencia: %w", err)
	}

	// 2. Checagem de mesma transação externa com outra chave
	var existingWithOtherKey string
	err = tx.QueryRow(ctx, "SELECT id FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2", wager.ProviderID, wager.ExternalTransactionID).Scan(&existingWithOtherKey)
	if err == nil {
		return nil, domain.ErrExternalKeyReused
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("erro ao consultar transacao externa existente: %w", err)
	}

	// 3. Lock pessimista da carteira
	var currentBalanceStr, walletCurrency string
	var version int
	err = tx.QueryRow(ctx, "SELECT balance, currency, version FROM wallets WHERE id = $1 FOR UPDATE", wager.WalletID).
		Scan(&currentBalanceStr, &walletCurrency, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("carteira nao encontrada")
		}
		return nil, fmt.Errorf("erro ao bloquear carteira: %w", err)
	}

	currentBalance, _ := domain.NewMoney(currentBalanceStr, walletCurrency)

	// Validar compatibilidade de moeda
	if wager.Money.Currency != walletCurrency {
		return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeCurrencyMismatch, correlationID)
	}

	newBalance := currentBalance
	shouldUpdateBalance := false
	var direction string

	switch wager.Kind {
	case domain.KindBet:
		if !wager.Money.Amount.IsPositive() {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidAmount, correlationID)
		}
		subBalance, errSub := currentBalance.Subtract(wager.Money)
		if errSub != nil || subBalance.Amount.IsNegative() {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInsufficientFunds, correlationID)
		}
		direction = "DEBIT"
		newBalance = subBalance
		shouldUpdateBalance = true
		wager.Status = domain.StatusProcessed

	case domain.KindWin:
		if !wager.Money.Amount.IsPositive() {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidAmount, correlationID)
		}
		addBalance, errAdd := currentBalance.Add(wager.Money)
		if errAdd != nil {
			return nil, fmt.Errorf("erro ao calcular saldo: %w", errAdd)
		}
		direction = "CREDIT"
		newBalance = addBalance
		shouldUpdateBalance = true
		wager.Status = domain.StatusProcessed

	case domain.KindLoss:
		if !wager.Money.Amount.IsZero() {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidAmount, correlationID)
		}
		shouldUpdateBalance = false
		wager.Status = domain.StatusProcessed

	case domain.KindRefund:
		if wager.ReferenceExternalTxID == "" {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidReference, correlationID)
		}
		if !wager.Money.Amount.IsPositive() {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidAmount, correlationID)
		}

		var refID, refWalletID, refPlayerID, refKind, refAmountStr, refCurrency, refStatus, refRoundID string
		errRef := tx.QueryRow(ctx, `
			SELECT id, wallet_id, player_id, kind, amount, currency, status, COALESCE(round_id, '')
			FROM wager_transactions
			WHERE provider_id = $1 AND external_transaction_id = $2`,
			wager.ProviderID, wager.ReferenceExternalTxID).
			Scan(&refID, &refWalletID, &refPlayerID, &refKind, &refAmountStr, &refCurrency, &refStatus, &refRoundID)

		if errRef != nil {
			if errors.Is(errRef, pgx.ErrNoRows) {
				wager.Status = domain.StatusPendingReference
				shouldUpdateBalance = false
				break
			}
			return nil, fmt.Errorf("erro ao consultar referencia de refund: %w", errRef)
		}

		if refStatus == string(domain.StatusPendingReference) || refStatus == string(domain.StatusPending) {
			wager.Status = domain.StatusPendingReference
			shouldUpdateBalance = false
			break
		}

		if refStatus != string(domain.StatusProcessed) || refKind != string(domain.KindBet) ||
			refWalletID != wager.WalletID || refPlayerID != wager.PlayerID || refCurrency != wager.Money.Currency {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidReference, correlationID)
		}

		refMoney, _ := domain.NewMoney(refAmountStr, refCurrency)
		if !wager.Money.Equal(refMoney) {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeAmountMismatch, correlationID)
		}

		var duplicateCount int
		errDup := tx.QueryRow(ctx, `
			SELECT COUNT(id) FROM wager_transactions 
			WHERE provider_id = $1 AND reference_external_tx_id = $2 AND kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED'`,
			wager.ProviderID, wager.ReferenceExternalTxID).Scan(&duplicateCount)
		if errDup == nil && duplicateCount > 0 {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeDuplicateReversal, correlationID)
		}

		direction = "CREDIT"
		newBalance, _ = currentBalance.Add(wager.Money)
		shouldUpdateBalance = true
		wager.Status = domain.StatusProcessed

	case domain.KindRollback:
		if wager.ReferenceExternalTxID == "" {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidReference, correlationID)
		}
		if !wager.Money.Amount.IsPositive() {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidAmount, correlationID)
		}

		var refID, refWalletID, refPlayerID, refKind, refAmountStr, refCurrency, refStatus string
		errRef := tx.QueryRow(ctx, `
			SELECT id, wallet_id, player_id, kind, amount, currency, status
			FROM wager_transactions
			WHERE provider_id = $1 AND external_transaction_id = $2`,
			wager.ProviderID, wager.ReferenceExternalTxID).
			Scan(&refID, &refWalletID, &refPlayerID, &refKind, &refAmountStr, &refCurrency, &refStatus)

		if errRef != nil {
			if errors.Is(errRef, pgx.ErrNoRows) {
				wager.Status = domain.StatusPendingReference
				shouldUpdateBalance = false
				break
			}
			return nil, fmt.Errorf("erro ao consultar referencia de rollback: %w", errRef)
		}

		if refStatus == string(domain.StatusPendingReference) || refStatus == string(domain.StatusPending) {
			wager.Status = domain.StatusPendingReference
			shouldUpdateBalance = false
			break
		}

		if refStatus != string(domain.StatusProcessed) || (refKind != string(domain.KindBet) && refKind != string(domain.KindWin) && refKind != string(domain.KindRefund)) ||
			refWalletID != wager.WalletID || refPlayerID != wager.PlayerID || refCurrency != wager.Money.Currency {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidReference, correlationID)
		}

		refMoney, _ := domain.NewMoney(refAmountStr, refCurrency)
		if !wager.Money.Equal(refMoney) {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeAmountMismatch, correlationID)
		}

		var duplicateCount int
		errDup := tx.QueryRow(ctx, `
			SELECT COUNT(id) FROM wager_transactions 
			WHERE provider_id = $1 AND reference_external_tx_id = $2 AND kind = 'ROLLBACK' AND status = 'PROCESSED'`,
			wager.ProviderID, wager.ReferenceExternalTxID).Scan(&duplicateCount)
		if errDup == nil && duplicateCount > 0 {
			return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeDuplicateReversal, correlationID)
		}

		if refKind == string(domain.KindBet) {
			direction = "CREDIT"
			newBalance, _ = currentBalance.Add(wager.Money)
		} else {
			direction = "DEBIT"
			subBal, errSub := currentBalance.Subtract(wager.Money)
			if errSub != nil || subBal.Amount.IsNegative() {
				return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeReversalInsufficientFunds, correlationID)
			}
			newBalance = subBal
		}
		shouldUpdateBalance = true
		wager.Status = domain.StatusProcessed

	default:
		return r.persistRejection(ctx, tx, wager, idempotencyKey, payloadHash, currentBalance, domain.FailureCodeInvalidKind, correlationID)
	}

	now := time.Now().UTC()
	if wager.Status == domain.StatusProcessed {
		wager.FinishedAt = &now
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO wager_transactions 
		(id, wallet_id, provider_id, external_transaction_id, idempotency_key, payload_hash, player_id, round_id, game_id, kind, status, failure_code, amount, currency, reference_external_tx_id, created_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
		wager.ID, wager.WalletID, wager.ProviderID, wager.ExternalTransactionID, idempotencyKey, payloadHash, wager.PlayerID,
		wager.RoundID, wager.GameID, string(wager.Kind), string(wager.Status), wager.FailureCode,
		wager.Money.Amount.StringFixed(2), wager.Money.Currency, wager.ReferenceExternalTxID, wager.CreatedAt, wager.FinishedAt)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			var existingHash string
			errQuery := tx.QueryRow(ctx, "SELECT payload_hash FROM wager_transactions WHERE idempotency_key = $1", idempotencyKey).Scan(&existingHash)
			if errQuery == nil && existingHash != payloadHash {
				return nil, domain.ErrIdempotencyConflict
			}
			return &domain.ProcessResult{
				TransactionID:    wager.ID,
				Status:           wager.Status,
				Balance:          currentBalance,
				IdempotentReplay: true,
			}, nil
		}
		return nil, fmt.Errorf("erro ao inserir transacao: %w", err)
	}

	if shouldUpdateBalance {
		ledgerID := uuid.New().String()
		_, err = tx.Exec(ctx, `
			INSERT INTO wallet_ledger (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			ledgerID, wager.WalletID, wager.ID, direction, wager.Money.Amount.StringFixed(2), currentBalance.Amount.StringFixed(2), newBalance.Amount.StringFixed(2), now)
		if err != nil {
			return nil, fmt.Errorf("erro ao inserir ledger: %w", err)
		}

		_, err = tx.Exec(ctx, "UPDATE wallets SET balance = $1, version = version + 1, updated_at = $2 WHERE id = $3", newBalance.Amount.StringFixed(2), now, wager.WalletID)
		if err != nil {
			return nil, fmt.Errorf("erro ao atualizar saldo da carteira: %w", err)
		}

		if err := r.insertOutboxBalanceChanged(ctx, tx, wager.WalletID, wager.ID, direction, wager.Money, currentBalance.Amount.StringFixed(2), newBalance.Amount.StringFixed(2), version+1, correlationID); err != nil {
			return nil, fmt.Errorf("falha ao inserir evento WalletBalanceChanged na outbox: %w", err)
		}
	}

	if wager.Status == domain.StatusPendingReference {
		if err := r.insertOutboxPendingReference(ctx, tx, wager, correlationID); err != nil {
			return nil, fmt.Errorf("falha ao inserir evento WagerTransactionPendingReference: %w", err)
		}
	} else if wager.Status == domain.StatusProcessed {
		if err := r.insertOutboxProcessed(ctx, tx, wager, correlationID); err != nil {
			return nil, fmt.Errorf("falha ao inserir evento WagerTransactionProcessed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("falha no commit da operacao: %w", err)
	}

	return &domain.ProcessResult{
		TransactionID:    wager.ID,
		Status:           wager.Status,
		Balance:          newBalance,
		IdempotentReplay: false,
	}, nil
}

func (r *WalletRepository) persistRejection(ctx context.Context, tx pgx.Tx, wager domain.WagerTransaction, idempotencyKey, payloadHash string, currentBalance domain.Money, failureCode, correlationID string) (*domain.ProcessResult, error) {
	now := time.Now().UTC()
	wager.Status = domain.StatusRejected
	wager.FailureCode = failureCode
	wager.FinishedAt = &now

	_, err := tx.Exec(ctx, `
		INSERT INTO wager_transactions 
		(id, wallet_id, provider_id, external_transaction_id, idempotency_key, payload_hash, player_id, round_id, game_id, kind, status, failure_code, amount, currency, reference_external_tx_id, created_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
		wager.ID, wager.WalletID, wager.ProviderID, wager.ExternalTransactionID, idempotencyKey, payloadHash, wager.PlayerID,
		wager.RoundID, wager.GameID, string(wager.Kind), string(wager.Status), wager.FailureCode,
		wager.Money.Amount.StringFixed(2), wager.Money.Currency, wager.ReferenceExternalTxID, wager.CreatedAt, wager.FinishedAt)

	if err != nil {
		return nil, fmt.Errorf("erro ao registrar rejeicao: %w", err)
	}

	if err := r.insertOutboxRejected(ctx, tx, wager, correlationID); err != nil {
		return nil, fmt.Errorf("falha ao inserir outbox de rejeicao: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("falha no commit da rejeicao: %w", err)
	}

	return &domain.ProcessResult{
		TransactionID:    wager.ID,
		Status:           domain.StatusRejected,
		Balance:          currentBalance,
		IdempotentReplay: false,
		FailureCode:      failureCode,
	}, nil
}

func (r *WalletRepository) ResolveNextPendingReference(ctx context.Context, ttl time.Duration) (bool, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("falha ao iniciar transacao: %w", err)
	}
	defer tx.Rollback(ctx)

	var wager domain.WagerTransaction
	var amountStr string
	err = tx.QueryRow(ctx, `
		SELECT id, wallet_id, provider_id, external_transaction_id, reference_external_tx_id, player_id, COALESCE(round_id, ''), COALESCE(game_id, ''), kind, amount, currency, created_at 
		FROM wager_transactions 
		WHERE status = $1 
		ORDER BY created_at ASC 
		FOR UPDATE SKIP LOCKED LIMIT 1`, string(domain.StatusPendingReference)).
		Scan(&wager.ID, &wager.WalletID, &wager.ProviderID, &wager.ExternalTransactionID, &wager.ReferenceExternalTxID, &wager.PlayerID, &wager.RoundID, &wager.GameID, &wager.Kind, &amountStr, &wager.Money.Currency, &wager.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("erro ao buscar pendencia: %w", err)
	}

	wager.Status = domain.StatusPendingReference
	wager.Money, err = domain.NewMoney(amountStr, wager.Money.Currency)
	if err != nil {
		return false, fmt.Errorf("erro ao converter valor da transacao: %w", err)
	}

	now := time.Now().UTC()

	// 1. Validação de TTL
	if time.Since(wager.CreatedAt) > ttl {
		wager.Status = domain.StatusRejected
		wager.FailureCode = domain.FailureCodeReferenceTimeout
		wager.FinishedAt = &now

		_, err = tx.Exec(ctx, "UPDATE wager_transactions SET status = $1, failure_code = $2, finished_at = $3 WHERE id = $4",
			string(domain.StatusRejected), wager.FailureCode, wager.FinishedAt, wager.ID)
		if err != nil {
			return false, fmt.Errorf("falha ao atualizar status para REJECTED por TTL: %w", err)
		}

		if err := r.insertOutboxRejected(ctx, tx, wager, ""); err != nil {
			return false, fmt.Errorf("falha ao inserir outbox event de rejeicao: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("falha no commit ao expirar por ttl: %w", err)
		}
		return true, nil
	}

	// 2. Busca da Referência
	var refID, refWalletID, refPlayerID, refKind, refAmountStr, refCurrency, refStatus string
	err = tx.QueryRow(ctx, `
		SELECT id, wallet_id, player_id, kind, amount, currency, status
		FROM wager_transactions 
		WHERE provider_id = $1 AND external_transaction_id = $2`,
		wager.ProviderID, wager.ReferenceExternalTxID).
		Scan(&refID, &refWalletID, &refPlayerID, &refKind, &refAmountStr, &refCurrency, &refStatus)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil // Referência ainda não chegou
		}
		return false, fmt.Errorf("erro ao consultar referencia: %w", err)
	}

	if refStatus == string(domain.StatusPendingReference) || refStatus == string(domain.StatusPending) {
		return true, nil // Referência ainda pendente
	}

	if refStatus != string(domain.StatusProcessed) {
		wager.Status = domain.StatusRejected
		wager.FailureCode = domain.FailureCodeInvalidReference
		wager.FinishedAt = &now

		_, err = tx.Exec(ctx, "UPDATE wager_transactions SET status = $1, failure_code = $2, finished_at = $3 WHERE id = $4",
			string(domain.StatusRejected), wager.FailureCode, wager.FinishedAt, wager.ID)
		if err != nil {
			return false, err
		}
		if err := r.insertOutboxRejected(ctx, tx, wager, ""); err != nil {
			return false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}

	refMoney, err := domain.NewMoney(refAmountStr, refCurrency)
	if err != nil || !wager.Money.Equal(refMoney) {
		wager.Status = domain.StatusRejected
		wager.FailureCode = domain.FailureCodeAmountMismatch
		wager.FinishedAt = &now

		_, err = tx.Exec(ctx, "UPDATE wager_transactions SET status = $1, failure_code = $2, finished_at = $3 WHERE id = $4",
			string(domain.StatusRejected), wager.FailureCode, wager.FinishedAt, wager.ID)
		if err != nil {
			return false, err
		}
		if err := r.insertOutboxRejected(ctx, tx, wager, ""); err != nil {
			return false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}

	// 3. Lock da carteira
	var currentBalanceStr, walletCurr string
	var version int
	err = tx.QueryRow(ctx, "SELECT balance, currency, version FROM wallets WHERE id = $1 FOR UPDATE", wager.WalletID).
		Scan(&currentBalanceStr, &walletCurr, &version)
	if err != nil {
		return false, fmt.Errorf("carteira nao encontrada ou erro de lock: %w", err)
	}

	currentBalance, _ := domain.NewMoney(currentBalanceStr, walletCurr)

	var newBalance domain.Money
	var direction string

	switch wager.Kind {
	case domain.KindRefund:
		if refKind != string(domain.KindBet) {
			wager.Status = domain.StatusRejected
			wager.FailureCode = domain.FailureCodeInvalidReference
			wager.FinishedAt = &now
			_, _ = tx.Exec(ctx, "UPDATE wager_transactions SET status = $1, failure_code = $2, finished_at = $3 WHERE id = $4",
				string(domain.StatusRejected), wager.FailureCode, wager.FinishedAt, wager.ID)
			_ = r.insertOutboxRejected(ctx, tx, wager, "")
			_ = tx.Commit(ctx)
			return true, nil
		}

		direction = "CREDIT"
		newBalance, _ = currentBalance.Add(wager.Money)

	case domain.KindRollback:
		if refKind == string(domain.KindBet) {
			direction = "CREDIT"
			newBalance, _ = currentBalance.Add(wager.Money)
		} else if refKind == string(domain.KindWin) || refKind == string(domain.KindRefund) {
			direction = "DEBIT"
			subBal, errSub := currentBalance.Subtract(wager.Money)
			if errSub != nil || subBal.Amount.IsNegative() {
				wager.Status = domain.StatusRejected
				wager.FailureCode = domain.FailureCodeReversalInsufficientFunds
				wager.FinishedAt = &now

				_, err = tx.Exec(ctx, "UPDATE wager_transactions SET status = $1, failure_code = $2, finished_at = $3 WHERE id = $4",
					string(domain.StatusRejected), wager.FailureCode, wager.FinishedAt, wager.ID)
				if err != nil {
					return false, err
				}
				if err := r.insertOutboxRejected(ctx, tx, wager, ""); err != nil {
					return false, err
				}
				if err := tx.Commit(ctx); err != nil {
					return false, err
				}
				return true, nil
			}
			newBalance = subBal
		} else {
			wager.Status = domain.StatusRejected
			wager.FailureCode = domain.FailureCodeInvalidReference
			wager.FinishedAt = &now
			_, _ = tx.Exec(ctx, "UPDATE wager_transactions SET status = $1, failure_code = $2, finished_at = $3 WHERE id = $4",
				string(domain.StatusRejected), wager.FailureCode, wager.FinishedAt, wager.ID)
			_ = r.insertOutboxRejected(ctx, tx, wager, "")
			_ = tx.Commit(ctx)
			return true, nil
		}
	}

	// 4. Persistência
	ledgerID := uuid.New().String()
	_, err = tx.Exec(ctx, `
		INSERT INTO wallet_ledger (id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		ledgerID, wager.WalletID, wager.ID, direction, wager.Money.Amount.StringFixed(2), currentBalance.Amount.StringFixed(2), newBalance.Amount.StringFixed(2), now)
	if err != nil {
		return false, fmt.Errorf("erro ao inserir registro no ledger: %w", err)
	}

	_, err = tx.Exec(ctx, "UPDATE wallets SET balance = $1, version = version + 1, updated_at = $2 WHERE id = $3", newBalance.Amount.StringFixed(2), now, wager.WalletID)
	if err != nil {
		return false, fmt.Errorf("erro ao atualizar saldo da carteira: %w", err)
	}

	if err := r.insertOutboxBalanceChanged(ctx, tx, wager.WalletID, wager.ID, direction, wager.Money, currentBalance.Amount.StringFixed(2), newBalance.Amount.StringFixed(2), version+1, ""); err != nil {
		return false, fmt.Errorf("falha ao inserir evento WalletBalanceChanged na outbox: %w", err)
	}

	wager.Status = domain.StatusProcessed
	wager.FinishedAt = &now
	_, err = tx.Exec(ctx, "UPDATE wager_transactions SET status = $1, finished_at = $2 WHERE id = $3", string(domain.StatusProcessed), wager.FinishedAt, wager.ID)
	if err != nil {
		return false, fmt.Errorf("erro ao atualizar status para PROCESSED: %w", err)
	}

	if err := r.insertOutboxProcessed(ctx, tx, wager, ""); err != nil {
		return false, fmt.Errorf("falha ao inserir outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("falha no commit da transacao: %w", err)
	}

	return true, nil
}

func (r *WalletRepository) insertOutboxProcessed(ctx context.Context, tx pgx.Tx, wager domain.WagerTransaction, correlationID string) error {
	data := domain.WagerTransactionProcessedData{
		TransactionID: wager.ID,
		WalletID:      wager.WalletID,
		ProviderID:    wager.ProviderID,
		Kind:          wager.Kind,
		Status:        wager.Status,
		Money:         wager.Money,
	}
	env, err := domain.NewOutboxEventEnvelope("WagerTransactionProcessed", wager.WalletID, correlationID, "", 1, data)
	if err != nil {
		return err
	}
	payloadBytes, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO outbox (id, aggregate_id, event_type, payload) VALUES ($1, $2, $3, $4)", env.EventID, wager.WalletID, env.EventType, payloadBytes)
	return err
}

func (r *WalletRepository) insertOutboxRejected(ctx context.Context, tx pgx.Tx, wager domain.WagerTransaction, correlationID string) error {
	data := domain.WagerTransactionRejectedData{
		TransactionID: wager.ID,
		WalletID:      wager.WalletID,
		ProviderID:    wager.ProviderID,
		Kind:          wager.Kind,
		Status:        wager.Status,
		FailureCode:   wager.FailureCode,
		Money:         wager.Money,
	}
	env, err := domain.NewOutboxEventEnvelope("WagerTransactionRejected", wager.WalletID, correlationID, "", 1, data)
	if err != nil {
		return err
	}
	payloadBytes, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO outbox (id, aggregate_id, event_type, payload) VALUES ($1, $2, $3, $4)", env.EventID, wager.WalletID, env.EventType, payloadBytes)
	return err
}

func (r *WalletRepository) insertOutboxPendingReference(ctx context.Context, tx pgx.Tx, wager domain.WagerTransaction, correlationID string) error {
	data := domain.WagerTransactionPendingReferenceData{
		TransactionID:                  wager.ID,
		WalletID:                       wager.WalletID,
		ProviderID:                     wager.ProviderID,
		Kind:                           wager.Kind,
		Status:                         wager.Status,
		ReferenceExternalTransactionID: wager.ReferenceExternalTxID,
		Money:                          wager.Money,
	}
	env, err := domain.NewOutboxEventEnvelope("WagerTransactionPendingReference", wager.WalletID, correlationID, "", 1, data)
	if err != nil {
		return err
	}
	payloadBytes, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO outbox (id, aggregate_id, event_type, payload) VALUES ($1, $2, $3, $4)", env.EventID, wager.WalletID, env.EventType, payloadBytes)
	return err
}

func (r *WalletRepository) insertOutboxBalanceChanged(ctx context.Context, tx pgx.Tx, walletID, txID, direction string, money domain.Money, balanceBefore, balanceAfter string, walletVersion int, correlationID string) error {
	data := domain.WalletBalanceChangedData{
		WalletID:      walletID,
		TransactionID: txID,
		Direction:     direction,
		Money:         money,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
		WalletVersion: walletVersion,
	}
	env, err := domain.NewOutboxEventEnvelope("WalletBalanceChanged", walletID, correlationID, "", walletVersion, data)
	if err != nil {
		return err
	}
	payloadBytes, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO outbox (id, aggregate_id, event_type, payload) VALUES ($1, $2, $3, $4)", env.EventID, walletID, env.EventType, payloadBytes)
	return err
}

func (r *WalletRepository) GetWalletByID(ctx context.Context, id string) (domain.Wallet, error) {
	var w domain.Wallet
	var amountStr string
	err := r.db.QueryRow(ctx, "SELECT id, player_id, currency, balance, version, created_at, updated_at FROM wallets WHERE id = $1", id).
		Scan(&w.ID, &w.PlayerID, &w.Balance.Currency, &amountStr, &w.Version, &w.CreatedAt, &w.UpdatedAt)
	if err != nil {
		return domain.Wallet{}, err
	}
	w.Balance, _ = domain.NewMoney(amountStr, w.Balance.Currency)
	return w, nil
}

func (r *WalletRepository) GetLedger(ctx context.Context, walletID string, cursor time.Time, limit int) ([]domain.LedgerEntry, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id, wallet_id, transaction_id, direction, amount, balance_before, balance_after, created_at 
		FROM wallet_ledger 
		WHERE wallet_id = $1 AND created_at < $2
		ORDER BY created_at DESC, id DESC
		LIMIT $3`, walletID, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []domain.LedgerEntry
	for rows.Next() {
		var e domain.LedgerEntry
		if err := rows.Scan(&e.ID, &e.WalletID, &e.TransactionID, &e.Direction, &e.Amount, &e.BalanceBefore, &e.BalanceAfter, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func (r *WalletRepository) Reconcile(ctx context.Context, walletID string) (domain.ReconciliationReport, error) {
	var storedBalanceStr, currency string
	err := r.db.QueryRow(ctx, "SELECT balance, currency FROM wallets WHERE id = $1", walletID).Scan(&storedBalanceStr, &currency)
	if err != nil {
		return domain.ReconciliationReport{}, err
	}

	var calculatedStr string
	var count int
	err = r.db.QueryRow(ctx, `
		SELECT 
			COALESCE(SUM(amount) FILTER (WHERE direction = 'CREDIT'), 0) - 
			COALESCE(SUM(amount) FILTER (WHERE direction = 'DEBIT'), 0),
			COUNT(id)
		FROM wallet_ledger WHERE wallet_id = $1`, walletID).Scan(&calculatedStr, &count)
	if err != nil {
		return domain.ReconciliationReport{}, err
	}

	stored, _ := decimal.NewFromString(storedBalanceStr)
	calculated, _ := decimal.NewFromString(calculatedStr)
	diff := stored.Sub(calculated)

	return domain.ReconciliationReport{
		WalletID:          walletID,
		StoredBalance:     stored.StringFixed(2),
		CalculatedBalance: calculated.StringFixed(2),
		Difference:        diff.StringFixed(2),
		Currency:          currency,
		Consistent:        diff.IsZero(),
		CheckedEntries:    count,
	}, nil
}

func (r *WalletRepository) GetTransactionByID(ctx context.Context, txID string) (domain.WagerTransaction, error) {
	var wager domain.WagerTransaction
	var amountStr string
	query := `
		SELECT id, wallet_id, provider_id, external_transaction_id, COALESCE(reference_external_tx_id, ''),
		       player_id, COALESCE(round_id, ''), COALESCE(game_id, ''), kind, amount, currency, status, COALESCE(failure_code, ''), created_at, finished_at
		FROM wager_transactions
		WHERE id = $1
	`
	err := r.db.QueryRow(ctx, query, txID).Scan(
		&wager.ID, &wager.WalletID, &wager.ProviderID, &wager.ExternalTransactionID, &wager.ReferenceExternalTxID,
		&wager.PlayerID, &wager.RoundID, &wager.GameID, &wager.Kind, &amountStr, &wager.Money.Currency, &wager.Status,
		&wager.FailureCode, &wager.CreatedAt, &wager.FinishedAt,
	)
	if err != nil {
		return domain.WagerTransaction{}, err
	}
	wager.Money, err = domain.NewMoney(amountStr, wager.Money.Currency)
	if err != nil {
		return domain.WagerTransaction{}, fmt.Errorf("falha ao converter valor monetario da transacao: %w", err)
	}
	return wager, nil
}

func (r *WalletRepository) GetTransactionByExternalID(ctx context.Context, providerID, externalTxID string) (domain.WagerTransaction, error) {
	var wager domain.WagerTransaction
	var amountStr string
	query := `
		SELECT id, wallet_id, provider_id, external_transaction_id, COALESCE(reference_external_tx_id, ''),
		       player_id, COALESCE(round_id, ''), COALESCE(game_id, ''), kind, amount, currency, status, COALESCE(failure_code, ''), created_at, finished_at
		FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2
	`
	err := r.db.QueryRow(ctx, query, providerID, externalTxID).Scan(
		&wager.ID, &wager.WalletID, &wager.ProviderID, &wager.ExternalTransactionID, &wager.ReferenceExternalTxID,
		&wager.PlayerID, &wager.RoundID, &wager.GameID, &wager.Kind, &amountStr, &wager.Money.Currency, &wager.Status,
		&wager.FailureCode, &wager.CreatedAt, &wager.FinishedAt,
	)
	if err != nil {
		return domain.WagerTransaction{}, err
	}
	wager.Money, err = domain.NewMoney(amountStr, wager.Money.Currency)
	if err != nil {
		return domain.WagerTransaction{}, fmt.Errorf("falha ao converter valor monetario da transacao: %w", err)
	}
	return wager, nil
}
