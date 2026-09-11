package domain

import "errors"

var (
	// ErrIdempotencyConflict ocorre quando o provedor envia a mesma Idempotency-Key
	// mas com um payload (hash) diferente da requisição original.
	ErrIdempotencyConflict = errors.New("conflito de idempotencia: payload diferente para a mesma chave")

	// ErrIdempotentReplay ocorre quando o provedor reenvia exatamente a mesma requisição.
	// O sistema não deve processar novamente, apenas retornar o sucesso original.
	ErrIdempotentReplay = errors.New("replay idempotente: transacao ja processada com sucesso")

	// ErrExternalKeyReused ocorre quando o mesmo provider e external_tx_id é enviado com chave de idempotencia diferente.
	ErrExternalKeyReused = errors.New("chave externa ja utilizada com outra idempotency key")
)

type ProcessResult struct {
	TransactionID    string   `json:"transactionId"`
	Status           TxStatus `json:"status"`
	Balance          Money    `json:"balance"`
	IdempotentReplay bool     `json:"idempotentReplay"`
	FailureCode      string   `json:"failureCode,omitempty"`
}
