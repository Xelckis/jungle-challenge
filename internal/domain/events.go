package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type OutboxEventEnvelope struct {
	EventID       string `json:"eventId"`
	EventType     string `json:"eventType"`
	AggregateID   string `json:"aggregateId"`
	CorrelationID string `json:"correlationId,omitempty"`
	CausationID   string `json:"causationId,omitempty"`
	OccurredAt    string `json:"occurredAt"`
	Version       int    `json:"version"`
	Data          any    `json:"data"`
}

type WagerTransactionProcessedData struct {
	TransactionID string   `json:"transactionId"`
	WalletID      string   `json:"walletId"`
	ProviderID    string   `json:"providerId"`
	Kind          TxKind   `json:"kind"`
	Status        TxStatus `json:"status"`
	Money         Money    `json:"money"`
}

type WagerTransactionRejectedData struct {
	TransactionID string   `json:"transactionId"`
	WalletID      string   `json:"walletId"`
	ProviderID    string   `json:"providerId"`
	Kind          TxKind   `json:"kind"`
	Status        TxStatus `json:"status"`
	FailureCode   string   `json:"failureCode"`
	Money         Money    `json:"money"`
}

type WagerTransactionPendingReferenceData struct {
	TransactionID                  string   `json:"transactionId"`
	WalletID                       string   `json:"walletId"`
	ProviderID                     string   `json:"providerId"`
	Kind                           TxKind   `json:"kind"`
	Status                         TxStatus `json:"status"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId"`
	Money                          Money    `json:"money"`
}

type WalletBalanceChangedData struct {
	WalletID      string `json:"walletId"`
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Money         Money  `json:"money"`
	BalanceBefore string `json:"balanceBefore"`
	BalanceAfter  string `json:"balanceAfter"`
	WalletVersion int    `json:"walletVersion"`
}

func NewOutboxEventEnvelope(eventType, aggregateID, correlationID, causationID string, version int, data any) (OutboxEventEnvelope, error) {
	return OutboxEventEnvelope{
		EventID:       uuid.New().String(),
		EventType:     eventType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Version:       version,
		Data:          data,
	}, nil
}

type CanonicalPayload struct {
	ExternalTransactionID          string `json:"externalTransactionId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	MoneyAmount                    string `json:"moneyAmount"`
	MoneyCurrency                  string `json:"moneyCurrency"`
	PlayerID                       string `json:"playerId"`
	ProviderID                     string `json:"providerId"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	RoundID                        string `json:"roundId"`
	WalletID                       string `json:"walletId"`
}

func ComputePayloadHash(providerID, externalTxID, playerID, walletID, roundID, gameID, kind string, money Money, refExternalTxID string) string {
	canonical := CanonicalPayload{
		ExternalTransactionID:          externalTxID,
		GameID:                         gameID,
		Kind:                           kind,
		MoneyAmount:                    money.Amount.StringFixed(2),
		MoneyCurrency:                  money.Currency,
		PlayerID:                       playerID,
		ProviderID:                     providerID,
		ReferenceExternalTransactionID: refExternalTxID,
		RoundID:                        roundID,
		WalletID:                       walletID,
	}

	data, _ := json.Marshal(canonical)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
