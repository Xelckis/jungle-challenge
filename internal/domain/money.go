package domain

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/shopspring/decimal"
)

var (
	ErrInvalidCurrency = errors.New("moedas incompativeis")
	ErrNegativeValue   = errors.New("valor monetario nao pode ser negativo")
	ErrInvalidScale    = errors.New("escala monetaria excede duas casas decimais")
	ErrInvalidAmount   = errors.New("formato de valor monetario invalido")
)

type Money struct {
	Amount   decimal.Decimal `json:"amount"`
	Currency string          `json:"currency"`
}

func NewMoney(amountStr string, currency string) (Money, error) {
	if strings.TrimSpace(amountStr) == "" || strings.TrimSpace(currency) == "" {
		return Money{}, ErrInvalidAmount
	}

	currency = strings.ToUpper(strings.TrimSpace(currency))
	if len(currency) != 3 {
		return Money{}, ErrInvalidCurrency
	}

	// Rejeita notação científica e strings especiais como NaN ou Inf
	if strings.ContainsAny(amountStr, "eENaNInf") {
		return Money{}, ErrInvalidAmount
	}

	amount, err := decimal.NewFromString(amountStr)
	if err != nil {
		return Money{}, ErrInvalidAmount
	}

	if amount.IsNegative() {
		return Money{}, ErrNegativeValue
	}

	if amount.Exponent() < -2 {
		return Money{}, ErrInvalidScale
	}

	return Money{
		Amount:   amount,
		Currency: currency,
	}, nil
}

func NewZeroMoney(currency string) (Money, error) {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if len(currency) != 3 {
		return Money{}, ErrInvalidCurrency
	}
	return Money{
		Amount:   decimal.Zero,
		Currency: currency,
	}, nil
}

func (m Money) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{
		Amount:   m.Amount.StringFixed(2),
		Currency: m.Currency,
	})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	var aux struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	money, err := NewMoney(aux.Amount, aux.Currency)
	if err != nil {
		return err
	}
	*m = money
	return nil
}

func (m Money) Add(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, ErrInvalidCurrency
	}
	return Money{
		Amount:   m.Amount.Add(other.Amount),
		Currency: m.Currency,
	}, nil
}

func (m Money) Subtract(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, ErrInvalidCurrency
	}
	return Money{
		Amount:   m.Amount.Sub(other.Amount),
		Currency: m.Currency,
	}, nil
}

func (m Money) Negate() Money {
	return Money{
		Amount:   m.Amount.Neg(),
		Currency: m.Currency,
	}
}

func (m Money) Equal(other Money) bool {
	return m.Currency == other.Currency && m.Amount.Equal(other.Amount)
}

func (m Money) Cmp(other Money) (int, error) {
	if m.Currency != other.Currency {
		return 0, ErrInvalidCurrency
	}
	return m.Amount.Cmp(other.Amount), nil
}

func (m Money) IsZero() bool {
	return m.Amount.IsZero()
}

func (m Money) IsPositive() bool {
	return m.Amount.IsPositive()
}

func (m Money) IsNegative() bool {
	return m.Amount.IsNegative()
}

func (m Money) String() string {
	return m.Amount.StringFixed(2)
}
