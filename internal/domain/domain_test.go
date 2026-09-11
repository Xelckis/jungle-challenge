package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"jungleChallenge/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMoney_ValidationAndLimits(t *testing.T) {
	t.Run("Criacao valida com duas casas decimais", func(t *testing.T) {
		m, err := domain.NewMoney("25.00", "BRL")
		require.NoError(t, err)
		assert.Equal(t, "25.00", m.String())
		assert.Equal(t, "BRL", m.Currency)
		assert.True(t, m.IsPositive())
		assert.False(t, m.IsZero())
	})

	t.Run("Rejeita valor negativo na criacao", func(t *testing.T) {
		_, err := domain.NewMoney("-10.00", "BRL")
		assert.ErrorIs(t, err, domain.ErrNegativeValue)
	})

	t.Run("Rejeita escala superior a duas casas decimais", func(t *testing.T) {
		_, err := domain.NewMoney("10.123", "BRL")
		assert.ErrorIs(t, err, domain.ErrInvalidScale)
	})

	t.Run("Rejeita notacao cientifica", func(t *testing.T) {
		_, err := domain.NewMoney("1e2", "BRL")
		assert.ErrorIs(t, err, domain.ErrInvalidAmount)

		_, err = domain.NewMoney("1E+2", "BRL")
		assert.ErrorIs(t, err, domain.ErrInvalidAmount)
	})

	t.Run("Rejeita NaN ou Infinity", func(t *testing.T) {
		_, err := domain.NewMoney("NaN", "BRL")
		assert.ErrorIs(t, err, domain.ErrInvalidAmount)

		_, err = domain.NewMoney("Inf", "BRL")
		assert.ErrorIs(t, err, domain.ErrInvalidAmount)
	})

	t.Run("Rejeita string vazia", func(t *testing.T) {
		_, err := domain.NewMoney("", "BRL")
		assert.ErrorIs(t, err, domain.ErrInvalidAmount)
	})

	t.Run("Rejeita moeda invalida", func(t *testing.T) {
		_, err := domain.NewMoney("10.00", "BR")
		assert.ErrorIs(t, err, domain.ErrInvalidCurrency)

		_, err = domain.NewMoney("10.00", "BRLX")
		assert.ErrorIs(t, err, domain.ErrInvalidCurrency)
	})

	t.Run("Criacao de Zero Money", func(t *testing.T) {
		zero, err := domain.NewZeroMoney("BRL")
		require.NoError(t, err)
		assert.True(t, zero.IsZero())
		assert.Equal(t, "0.00", zero.String())
	})

	t.Run("Operacoes aritmeticas com mesma moeda", func(t *testing.T) {
		m1, err := domain.NewMoney("10.50", "BRL")
		require.NoError(t, err)
		m2, err := domain.NewMoney("5.25", "BRL")
		require.NoError(t, err)

		sum, err := m1.Add(m2)
		require.NoError(t, err)
		assert.Equal(t, "15.75", sum.String())

		diff, err := m1.Subtract(m2)
		require.NoError(t, err)
		assert.Equal(t, "5.25", diff.String())

		neg := m1.Negate()
		assert.Equal(t, "-10.50", neg.String())
		assert.True(t, neg.IsNegative())

		cmp, err := m1.Cmp(m2)
		require.NoError(t, err)
		assert.Equal(t, 1, cmp)
		assert.False(t, m1.Equal(m2))
	})

	t.Run("Operacoes com moedas incompativeis falham", func(t *testing.T) {
		brl, err := domain.NewMoney("10.00", "BRL")
		require.NoError(t, err)
		usd, err := domain.NewMoney("10.00", "USD")
		require.NoError(t, err)

		_, err = brl.Add(usd)
		assert.ErrorIs(t, err, domain.ErrInvalidCurrency)

		_, err = brl.Subtract(usd)
		assert.ErrorIs(t, err, domain.ErrInvalidCurrency)

		_, err = brl.Cmp(usd)
		assert.ErrorIs(t, err, domain.ErrInvalidCurrency)
	})

	t.Run("Serializacao e Deserializacao JSON", func(t *testing.T) {
		m, err := domain.NewMoney("123.45", "BRL")
		require.NoError(t, err)

		data, err := json.Marshal(m)
		require.NoError(t, err)
		assert.JSONEq(t, `{"amount":"123.45","currency":"BRL"}`, string(data))

		var unmarshaled domain.Money
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)
		assert.True(t, m.Equal(unmarshaled))
	})
}

func TestWallet_InvariantsAndOperations(t *testing.T) {
	t.Run("Criacao e estado inicial", func(t *testing.T) {
		initial, err := domain.NewMoney("100.00", "BRL")
		require.NoError(t, err)
		w := domain.NewWallet("w-1", "p-1", initial)

		assert.Equal(t, "w-1", w.ID)
		assert.Equal(t, "p-1", w.PlayerID)
		assert.Equal(t, 1, w.Version)
		assert.Equal(t, "100.00", w.Balance.String())
	})

	t.Run("Reidratacao de carteira", func(t *testing.T) {
		bal, err := domain.NewMoney("250.00", "BRL")
		require.NoError(t, err)
		now := time.Now().UTC()
		w := domain.RehydrateWallet("w-1", "p-1", bal, 5, now, now)

		assert.Equal(t, 5, w.Version)
		assert.Equal(t, "250.00", w.Balance.String())
	})

	t.Run("Credit incrementa saldo e versao", func(t *testing.T) {
		initial, _ := domain.NewMoney("100.00", "BRL")
		w := domain.NewWallet("w-1", "p-1", initial)

		creditAmount, _ := domain.NewMoney("50.00", "BRL")
		err := w.Credit(creditAmount)
		require.NoError(t, err)
		assert.Equal(t, "150.00", w.Balance.String())
		assert.Equal(t, 2, w.Version)
	})

	t.Run("Debit decrementa saldo e versao", func(t *testing.T) {
		initial, _ := domain.NewMoney("100.00", "BRL")
		w := domain.NewWallet("w-1", "p-1", initial)

		debitAmount, _ := domain.NewMoney("40.00", "BRL")
		err := w.Debit(debitAmount)
		require.NoError(t, err)
		assert.Equal(t, "60.00", w.Balance.String())
		assert.Equal(t, 2, w.Version)
	})

	t.Run("Debit rejeita saldo insuficiente", func(t *testing.T) {
		initial, _ := domain.NewMoney("50.00", "BRL")
		w := domain.NewWallet("w-1", "p-1", initial)

		debitAmount, _ := domain.NewMoney("60.00", "BRL")
		err := w.Debit(debitAmount)
		assert.ErrorIs(t, err, domain.ErrInsufficientFunds)
		assert.Equal(t, "50.00", w.Balance.String())
		assert.Equal(t, 1, w.Version)
	})

	t.Run("Rejeita operacoes com moedas incompativeis", func(t *testing.T) {
		initial, _ := domain.NewMoney("100.00", "BRL")
		w := domain.NewWallet("w-1", "p-1", initial)

		usd, _ := domain.NewMoney("10.00", "USD")
		assert.ErrorIs(t, w.Credit(usd), domain.ErrInvalidCurrency)
		assert.ErrorIs(t, w.Debit(usd), domain.ErrInvalidCurrency)
	})
}

func TestWagerTransaction_StateMachine(t *testing.T) {
	money, _ := domain.NewMoney("10.00", "BRL")

	t.Run("Transicoes de estado validas: Pending para Processed", func(t *testing.T) {
		tx := domain.NewWagerTransaction("tx-1", "w-1", domain.KindBet, money)
		assert.Equal(t, domain.StatusPending, tx.Status)
		assert.False(t, tx.IsTerminal())

		err := tx.Process()
		require.NoError(t, err)
		assert.Equal(t, domain.StatusProcessed, tx.Status)
		assert.True(t, tx.IsTerminal())
		assert.NotNil(t, tx.FinishedAt)

		// Tentativa de alteracao em estado terminal falha
		err = tx.Reject(domain.FailureCodeInsufficientFunds)
		assert.ErrorIs(t, err, domain.ErrTerminalState)
	})

	t.Run("Pending para Rejected com FailureCode", func(t *testing.T) {
		tx := domain.NewWagerTransaction("tx-2", "w-1", domain.KindBet, money)
		err := tx.Reject(domain.FailureCodeInsufficientFunds)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusRejected, tx.Status)
		assert.Equal(t, domain.FailureCodeInsufficientFunds, tx.FailureCode)
		assert.True(t, tx.IsTerminal())

		err = tx.Process()
		assert.ErrorIs(t, err, domain.ErrTerminalState)
	})

	t.Run("Pending para PendingReference", func(t *testing.T) {
		tx := domain.NewWagerTransaction("tx-3", "w-1", domain.KindRefund, money)
		err := tx.WaitReference()
		require.NoError(t, err)
		assert.Equal(t, domain.StatusPendingReference, tx.Status)
		assert.False(t, tx.IsTerminal())

		// A partir de PendingReference pode ir para Processed
		err = tx.Process()
		require.NoError(t, err)
		assert.Equal(t, domain.StatusProcessed, tx.Status)
	})
}

func TestCanonicalHash_Determinism(t *testing.T) {
	money, _ := domain.NewMoney("25.00", "BRL")

	hash1 := domain.ComputePayloadHash("provider-a", "tx-1", "p-1", "w-1", "round-1", "game-1", "BET", money, "")
	hash2 := domain.ComputePayloadHash("provider-a", "tx-1", "p-1", "w-1", "round-1", "game-1", "BET", money, "")
	assert.Equal(t, hash1, hash2, "Hashes para os mesmos dados devem ser identicos")

	moneyDiff, _ := domain.NewMoney("30.00", "BRL")
	hashDiff := domain.ComputePayloadHash("provider-a", "tx-1", "p-1", "w-1", "round-1", "game-1", "BET", moneyDiff, "")
	assert.NotEqual(t, hash1, hashDiff, "Hashes para valores diferentes devem ser distintos")
}
