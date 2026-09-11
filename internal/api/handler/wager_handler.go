package handler

import (
	"errors"
	"net/http"

	"jungleChallenge/internal/domain"
	"jungleChallenge/internal/repository"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type WagerHandler struct {
	repo   *repository.WalletRepository
	logger *zap.Logger
}

func NewWagerHandler(repo *repository.WalletRepository, logger *zap.Logger) *WagerHandler {
	return &WagerHandler{repo: repo, logger: logger}
}

type WagerRequest struct {
	ProviderID            string `json:"providerId" binding:"required"`
	ExternalTransactionID string `json:"externalTransactionId" binding:"required"`
	PlayerID              string `json:"playerId" binding:"required"`
	WalletID              string `json:"walletId" binding:"required"`
	RoundID               string `json:"roundId"`
	GameID                string `json:"gameId"`
	Kind                  string `json:"kind" binding:"required"`
	Money                 struct {
		Amount   string `json:"amount" binding:"required"`
		Currency string `json:"currency" binding:"required"`
	} `json:"money" binding:"required"`
	ReferenceExternalTxID string `json:"referenceExternalTransactionId"`
}

func (h *WagerHandler) Process(c *gin.Context) {
	correlationID := c.GetHeader("X-Correlation-ID")
	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey == "" {
		h.logger.Error("Idempotency-Key obrigatorio",
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key obrigatorio"})
		return
	}

	var req WagerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("payload invalido",
			zap.Error(err),
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "payload invalido"})
		return
	}

	if req.Kind == string(domain.KindOpening) {
		h.logger.Warn("operacao OPENING rejeitada em endpoint externo",
			zap.String("providerId", req.ProviderID),
			zap.String("walletId", req.WalletID),
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "tipo OPENING e reservado para criacao interna de carteira"})
		return
	}

	authProviderID := c.GetString("providerId")
	if authProviderID != "" && authProviderID != req.ProviderID {
		h.logger.Error("sem permissao para este providerId",
			zap.String("authProviderId", authProviderID),
			zap.String("reqProviderId", req.ProviderID),
			zap.String("walletId", req.WalletID),
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusForbidden, gin.H{"error": "sem permissao para este providerId"})
		return
	}

	money, err := domain.NewMoney(req.Money.Amount, req.Money.Currency)
	if err != nil {
		h.logger.Error("erro ao validar objeto money",
			zap.Error(err),
			zap.String("providerId", req.ProviderID),
			zap.String("walletId", req.WalletID),
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}

	payloadHash := domain.ComputePayloadHash(
		req.ProviderID,
		req.ExternalTransactionID,
		req.PlayerID,
		req.WalletID,
		req.RoundID,
		req.GameID,
		req.Kind,
		money,
		req.ReferenceExternalTxID,
	)

	wager := domain.WagerTransaction{
		ID:                    uuid.New().String(),
		WalletID:              req.WalletID,
		ProviderID:            req.ProviderID,
		ExternalTransactionID: req.ExternalTransactionID,
		PlayerID:              req.PlayerID,
		RoundID:               req.RoundID,
		GameID:                req.GameID,
		Kind:                  domain.TxKind(req.Kind),
		Money:                 money,
		Status:                domain.StatusPending,
		ReferenceExternalTxID: req.ReferenceExternalTxID,
	}

	result, err := h.repo.ProcessTransaction(c.Request.Context(), wager, idempotencyKey, payloadHash, "", correlationID)
	if err != nil {
		if errors.Is(err, domain.ErrIdempotencyConflict) {
			h.logger.Error("conflito de idempotencia detectado",
				zap.Error(err),
				zap.String("providerId", req.ProviderID),
				zap.String("walletId", req.WalletID),
				zap.String("correlationId", correlationID),
			)
			c.JSON(http.StatusConflict, gin.H{"error": "conflito de idempotencia: payload diferente para a mesma chave"})
			return
		}
		if errors.Is(err, domain.ErrExternalKeyReused) {
			h.logger.Error("reutilizacao de transacao externa com chave diferente",
				zap.Error(err),
				zap.String("providerId", req.ProviderID),
				zap.String("walletId", req.WalletID),
				zap.String("correlationId", correlationID),
			)
			c.JSON(http.StatusConflict, gin.H{"error": "transacao externa ja registrada com outra idempotency key"})
			return
		}
		h.logger.Error("falha ao processar transacao",
			zap.Error(err),
			zap.String("providerId", req.ProviderID),
			zap.String("walletId", req.WalletID),
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if result.Status == domain.StatusRejected {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"transactionId": result.TransactionID,
			"status":        result.Status,
			"failureCode":   result.FailureCode,
			"balance": gin.H{
				"amount":   result.Balance.Amount.StringFixed(2),
				"currency": result.Balance.Currency,
			},
			"idempotentReplay": result.IdempotentReplay,
		})
		return
	}

	if result.Status == domain.StatusPendingReference {
		c.JSON(http.StatusOK, gin.H{
			"transactionId":    result.TransactionID,
			"status":           result.Status,
			"idempotentReplay": result.IdempotentReplay,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"transactionId": result.TransactionID,
		"status":        result.Status,
		"balance": gin.H{
			"amount":   result.Balance.Amount.StringFixed(2),
			"currency": result.Balance.Currency,
		},
		"idempotentReplay": result.IdempotentReplay,
	})
}

func (h *WagerHandler) GetInternalTransaction(c *gin.Context) {
	txID := c.Param("transactionId")
	correlationID := c.GetHeader("X-Correlation-ID")

	h.logger.Info("Consultando transacao interna",
		zap.String("transactionId", txID),
		zap.String("correlationId", correlationID),
	)

	tx, err := h.repo.GetTransactionByID(c.Request.Context(), txID)
	if err != nil {
		h.logger.Error("Transacao interna nao encontrada",
			zap.Error(err),
			zap.String("transactionId", txID),
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusNotFound, gin.H{"error": "transacao nao encontrada"})
		return
	}

	c.JSON(http.StatusOK, tx)
}

func (h *WagerHandler) GetExternalTransaction(c *gin.Context) {
	providerID := c.Param("providerId")
	externalTxID := c.Param("externalTransactionId")
	correlationID := c.GetHeader("X-Correlation-ID")
	authProviderID := c.GetString("providerId")

	h.logger.Info("Consultando transacao externa",
		zap.String("providerId", providerID),
		zap.String("externalTransactionId", externalTxID),
		zap.String("correlationId", correlationID),
	)

	if authProviderID != "" && authProviderID != providerID {
		h.logger.Error("sem permissao para este providerId",
			zap.String("providerId", providerID),
			zap.String("authProviderId", authProviderID),
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusForbidden, gin.H{"error": "sem permissao para este providerId"})
		return
	}

	tx, err := h.repo.GetTransactionByExternalID(c.Request.Context(), providerID, externalTxID)
	if err != nil {
		h.logger.Error("Transacao externa nao encontrada",
			zap.Error(err),
			zap.String("providerId", providerID),
			zap.String("externalTransactionId", externalTxID),
			zap.String("correlationId", correlationID),
		)
		c.JSON(http.StatusNotFound, gin.H{"error": "transacao nao encontrada"})
		return
	}

	c.JSON(http.StatusOK, tx)
}
