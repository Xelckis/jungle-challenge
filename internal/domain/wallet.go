package domain

import (
	"errors"
	"time"
)

var (
	ErrInsufficientFunds = errors.New("saldo insuficiente")
	ErrInvalidOperation  = errors.New("operacao invalida para a carteira")
)

type LedgerEntry struct {
	ID            string    `json:"id"`
	WalletID      string    `json:"walletId,omitempty"`
	TransactionID string    `json:"transactionId"`
	Direction     string    `json:"direction"`
	Amount        string    `json:"amount"`
	BalanceBefore string    `json:"balanceBefore"`
	BalanceAfter  string    `json:"balanceAfter"`
	CreatedAt     time.Time `json:"createdAt"`
}

type ReconciliationReport struct {
	WalletID          string `json:"walletId"`
	StoredBalance     string `json:"storedBalance"`
	CalculatedBalance string `json:"calculatedBalance"`
	Difference        string `json:"difference"`
	Currency          string `json:"currency"`
	Consistent        bool   `json:"consistent"`
	CheckedEntries    int    `json:"checkedEntries"`
}

type Wallet struct {
	ID        string
	PlayerID  string
	Balance   Money
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewWallet(id, playerID string, initialBalance Money) Wallet {
	now := time.Now().UTC()
	return Wallet{
		ID:        id,
		PlayerID:  playerID,
		Balance:   initialBalance,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func RehydrateWallet(id, playerID string, balance Money, version int, createdAt, updatedAt time.Time) Wallet {
	return Wallet{
		ID:        id,
		PlayerID:  playerID,
		Balance:   balance,
		Version:   version,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
}

func (w *Wallet) Debit(amount Money) error {
	if w.Balance.Currency != amount.Currency {
		return ErrInvalidCurrency
	}
	if !amount.IsPositive() {
		return ErrInvalidOperation
	}

	newBalance, err := w.Balance.Subtract(amount)
	if err != nil {
		return err
	}

	if newBalance.Amount.IsNegative() {
		return ErrInsufficientFunds
	}

	w.Balance = newBalance
	w.Version++
	w.UpdatedAt = time.Now().UTC()
	return nil
}

func (w *Wallet) Credit(amount Money) error {
	if w.Balance.Currency != amount.Currency {
		return ErrInvalidCurrency
	}
	if !amount.IsPositive() {
		return ErrInvalidOperation
	}

	newBalance, err := w.Balance.Add(amount)
	if err != nil {
		return err
	}

	w.Balance = newBalance
	w.Version++
	w.UpdatedAt = time.Now().UTC()
	return nil
}
