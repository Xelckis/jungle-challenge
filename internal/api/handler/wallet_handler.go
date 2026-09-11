package handler

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"jungleChallenge/internal/domain"
	"jungleChallenge/internal/repository"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

type WalletHandler struct {
	repo   *repository.WalletRepository
	logger *zap.Logger
}

func NewWalletHandler(repo *repository.WalletRepository, logger *zap.Logger) *WalletHandler {
	return &WalletHandler{
		repo:   repo,
		logger: logger,
	}
}

type CreateWalletRequest struct {
	PlayerID       string `json:"playerId" binding:"required"`
	InitialBalance struct {
		Amount   string `json:"amount" binding:"required"`
		Currency string `json:"currency" binding:"required"`
	} `json:"initialBalance" binding:"required"`
}

func (h *WalletHandler) GetWallet(c *gin.Context) {
	walletID := c.Param("walletId")
	wallet, err := h.repo.GetWalletByID(c.Request.Context(), walletID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "carteira nao encontrada"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":       wallet.ID,
		"playerId": wallet.PlayerID,
		"balance": gin.H{
			"amount":   wallet.Balance.Amount.StringFixed(2),
			"currency": wallet.Balance.Currency,
		},
		"version": wallet.Version,
	})
}

func (h *WalletHandler) GetLedger(c *gin.Context) {
	walletID := c.Param("walletId")

	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 || limit > 100 {
		limit = 50
	}

	cursorStr := c.Query("cursor")
	cursor := time.Now().UTC()
	if cursorStr != "" {
		parsedCursor, err := time.Parse(time.RFC3339Nano, cursorStr)
		if err == nil {
			cursor = parsedCursor
		}
	}

	entries, err := h.repo.GetLedger(c.Request.Context(), walletID, cursor, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao buscar ledger"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"entries": entries})
}

func (h *WalletHandler) Reconcile(c *gin.Context) {
	walletID := c.Param("walletId")
	report, err := h.repo.Reconcile(c.Request.Context(), walletID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "carteira nao encontrada para reconciliacao"})
		return
	}

	if !report.Consistent {
		h.logger.Warn("Divergencia encontrada na reconciliacao da carteira",
			zap.String("walletId", report.WalletID),
			zap.String("storedBalance", report.StoredBalance),
			zap.String("calculatedBalance", report.CalculatedBalance),
			zap.String("difference", report.Difference),
		)
	}

	c.JSON(http.StatusOK, gin.H{
		"walletId": report.WalletID,
		"storedBalance": gin.H{
			"amount":   report.StoredBalance,
			"currency": report.Currency,
		},
		"calculatedBalance": gin.H{
			"amount":   report.CalculatedBalance,
			"currency": report.Currency,
		},
		"difference": gin.H{
			"amount":   report.Difference,
			"currency": report.Currency,
		},
		"consistent":     report.Consistent,
		"checkedEntries": report.CheckedEntries,
	})
}

func (h *WalletHandler) Create(c *gin.Context) {
	var req CreateWalletRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "payload invalido ou campos ausentes"})
		return
	}

	initialMoney, err := domain.NewMoney(req.InitialBalance.Amount, req.InitialBalance.Currency)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}

	walletID := uuid.New().String()
	wallet := domain.NewWallet(walletID, req.PlayerID, initialMoney)

	if err := h.repo.Create(c.Request.Context(), wallet); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			c.JSON(http.StatusConflict, gin.H{"error": "carteira ja cadastrada para este jogador e moeda"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "falha ao criar carteira"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":       wallet.ID,
		"playerId": wallet.PlayerID,
		"balance": gin.H{
			"amount":   wallet.Balance.Amount.StringFixed(2),
			"currency": wallet.Balance.Currency,
		},
		"version": wallet.Version,
	})
}
