package domain

import (
	"errors"
	"time"
)

type TxStatus string
type TxKind string

const (
	StatusPending          TxStatus = "PENDING"
	StatusProcessed        TxStatus = "PROCESSED"
	StatusPendingReference TxStatus = "PENDING_REFERENCE"
	StatusRejected         TxStatus = "REJECTED"
	StatusFailed           TxStatus = "FAILED"

	KindBet      TxKind = "BET"
	KindWin      TxKind = "WIN"
	KindLoss     TxKind = "LOSS"
	KindRefund   TxKind = "REFUND"
	KindRollback TxKind = "ROLLBACK"
	KindOpening  TxKind = "OPENING"

	FailureCodeInsufficientFunds         = "INSUFFICIENT_FUNDS"
	FailureCodeReversalInsufficientFunds = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureCodeInvalidReference          = "INVALID_REFERENCE"
	FailureCodeAmountMismatch            = "AMOUNT_MISMATCH"
	FailureCodeDuplicateReversal         = "DUPLICATE_REVERSAL"
	FailureCodeReferenceTimeout          = "REFERENCE_TIMEOUT"
	FailureCodeCurrencyMismatch          = "CURRENCY_MISMATCH"
	FailureCodeInvalidKind               = "INVALID_KIND"
	FailureCodeInvalidAmount             = "INVALID_AMOUNT"
)

var (
	ErrTerminalState = errors.New("transacao em estado terminal nao pode ser alterada")
	ErrInvalidState  = errors.New("transicao de estado invalida")
)

type WagerTransaction struct {
	ID                    string     `json:"id"`
	WalletID              string     `json:"walletId"`
	ProviderID            string     `json:"providerId"`
	ExternalTransactionID string     `json:"externalTransactionId"`
	ReferenceExternalTxID string     `json:"referenceExternalTransactionId,omitempty"`
	PlayerID              string     `json:"playerId"`
	RoundID               string     `json:"roundId,omitempty"`
	GameID                string     `json:"gameId,omitempty"`
	Kind                  TxKind     `json:"kind"`
	Money                 Money      `json:"money"`
	Status                TxStatus   `json:"status"`
	FailureCode           string     `json:"failureCode,omitempty"`
	CreatedAt             time.Time  `json:"createdAt"`
	FinishedAt            *time.Time `json:"finishedAt,omitempty"`
}

func NewWagerTransaction(id, walletID string, kind TxKind, money Money) WagerTransaction {
	now := time.Now().UTC()
	return WagerTransaction{
		ID:        id,
		WalletID:  walletID,
		Kind:      kind,
		Money:     money,
		Status:    StatusPending,
		CreatedAt: now,
	}
}

func (t *WagerTransaction) IsTerminal() bool {
	return t.Status == StatusProcessed || t.Status == StatusRejected || t.Status == StatusFailed
}

func (t *WagerTransaction) Process() error {
	if t.IsTerminal() {
		return ErrTerminalState
	}
	t.Status = StatusProcessed
	now := time.Now().UTC()
	t.FinishedAt = &now
	return nil
}

func (t *WagerTransaction) Reject(failureCode string) error {
	if t.IsTerminal() {
		return ErrTerminalState
	}
	t.Status = StatusRejected
	t.FailureCode = failureCode
	now := time.Now().UTC()
	t.FinishedAt = &now
	return nil
}

func (t *WagerTransaction) WaitReference() error {
	if t.IsTerminal() {
		return ErrTerminalState
	}
	t.Status = StatusPendingReference
	return nil
}

func (t *WagerTransaction) Fail(failureCode string) error {
	if t.IsTerminal() {
		return ErrTerminalState
	}
	t.Status = StatusFailed
	t.FailureCode = failureCode
	now := time.Now().UTC()
	t.FinishedAt = &now
	return nil
}
