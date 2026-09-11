package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"jungleChallenge/internal/domain"
	"jungleChallenge/internal/repository"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type SQSMessageEnvelope struct {
	MessageID string `json:"messageId"`
	Type      string `json:"type"`
	Data      struct {
		ProviderID            string `json:"providerId"`
		ExternalTransactionID string `json:"externalTransactionId"`
		IdempotencyKey        string `json:"idempotencyKey"`
		PlayerID              string `json:"playerId"`
		WalletID              string `json:"walletId"`
		RoundID               string `json:"roundId"`
		GameID                string `json:"gameId"`
		Kind                  string `json:"kind"`
		Money                 struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		ReferenceExternalTxID string `json:"referenceExternalTransactionId"`
	} `json:"data"`
}

type InboxWorker struct {
	repo     *repository.WalletRepository
	sqs      *sqs.Client
	queueURL string
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	logger   *zap.Logger
}

func NewInboxWorker(repo *repository.WalletRepository, sqsClient *sqs.Client, logger *zap.Logger) *InboxWorker {
	queueURL := cmp.Or(os.Getenv("SQS_QUEUE_URL"), "http://127.0.0.1:4566/000000000000/wager-transactions.fifo")
	return &InboxWorker{
		repo:     repo,
		sqs:      sqsClient,
		queueURL: queueURL,
		logger:   logger,
	}
}

func (w *InboxWorker) Start() {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.logger.Info("Consumidor SQS (Inbox) iniciado...")
		for {
			select {
			case <-w.ctx.Done():
				w.logger.Info("Consumidor SQS (Inbox) encerrado...")
				return
			default:
				w.pollMessages(w.ctx)
			}
		}
	}()
}

func (w *InboxWorker) Stop(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *InboxWorker) pollMessages(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	msgResult, err := w.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(w.queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     5,
	})

	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}
		w.logger.Error("Erro ao puxar mensagens do SQS", zap.Error(err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			return
		}
	}

	for _, msg := range msgResult.Messages {
		if ctx.Err() != nil {
			return
		}
		w.processMessage(ctx, msg.ReceiptHandle, msg.Body)
	}
}

func (w *InboxWorker) processMessage(ctx context.Context, receiptHandle *string, body *string) {
	if body == nil || ctx.Err() != nil {
		return
	}

	var env SQSMessageEnvelope
	if err := json.Unmarshal([]byte(*body), &env); err != nil {
		w.logger.Error("Falha ao ler JSON da mensagem, descartando mensagem invalida", zap.Error(err))
		w.deleteMessage(ctx, receiptHandle, env)
		return
	}

	if env.Data.Kind == string(domain.KindOpening) {
		w.logger.Warn("Operacao OPENING rejeitada em mensagem SQS", zap.String("messageId", env.MessageID))
		w.deleteMessage(ctx, receiptHandle, env)
		return
	}

	money, err := domain.NewMoney(env.Data.Money.Amount, env.Data.Money.Currency)
	if err != nil {
		w.logger.Warn("Valor monetario invalido na msg SQS", zap.Error(err), zap.String("messageId", env.MessageID))
		w.deleteMessage(ctx, receiptHandle, env)
		return
	}

	payloadHash := domain.ComputePayloadHash(
		env.Data.ProviderID,
		env.Data.ExternalTransactionID,
		env.Data.PlayerID,
		env.Data.WalletID,
		env.Data.RoundID,
		env.Data.GameID,
		env.Data.Kind,
		money,
		env.Data.ReferenceExternalTxID,
	)

	wager := domain.WagerTransaction{
		ID:                    uuid.New().String(),
		WalletID:              env.Data.WalletID,
		ProviderID:            env.Data.ProviderID,
		ExternalTransactionID: env.Data.ExternalTransactionID,
		ReferenceExternalTxID: env.Data.ReferenceExternalTxID,
		PlayerID:              env.Data.PlayerID,
		RoundID:               env.Data.RoundID,
		GameID:                env.Data.GameID,
		Kind:                  domain.TxKind(env.Data.Kind),
		Money:                 money,
		Status:                domain.StatusPending,
	}

	result, err := w.repo.ProcessTransaction(ctx, wager, env.Data.IdempotencyKey, payloadHash, env.MessageID, "")
	if err != nil {
		if errors.Is(err, domain.ErrIdempotentReplay) || errors.Is(err, domain.ErrIdempotencyConflict) || errors.Is(err, domain.ErrExternalKeyReused) {
			w.deleteMessage(ctx, receiptHandle, env)
			return
		}
		w.logger.Error("Falha temporaria processando msg SQS (sera reentregue)", zap.Error(err), zap.String("messageId", env.MessageID))
		return
	}

	if result != nil {
		w.logger.Info("Mensagem SQS processada com sucesso",
			zap.String("messageId", env.MessageID),
			zap.String("transactionId", result.TransactionID),
			zap.String("status", string(result.Status)),
		)
	}

	w.deleteMessage(ctx, receiptHandle, env)
}

func (w *InboxWorker) deleteMessage(ctx context.Context, receiptHandle *string, env SQSMessageEnvelope) {
	_, err := w.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(w.queueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		if !errors.Is(ctx.Err(), context.Canceled) {
			w.logger.Error("Falha ao deletar mensagem do SQS", zap.Error(err), zap.String("messageId", env.MessageID))
		}
	}
}
