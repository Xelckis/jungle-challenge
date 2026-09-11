package worker

import (
	"cmp"
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type OutboxWorker struct {
	db       *pgxpool.Pool
	sqs      *sqs.Client
	queueURL string
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	logger   *zap.Logger
}

func NewOutboxWorker(db *pgxpool.Pool, sqsClient *sqs.Client, logger *zap.Logger) *OutboxWorker {
	queueURL := cmp.Or(os.Getenv("SQS_QUEUE_URL"), "http://127.0.0.1:4566/000000000000/wager-transactions.fifo")
	return &OutboxWorker{
		db:       db,
		sqs:      sqsClient,
		queueURL: queueURL,
		logger:   logger,
	}
}

func (w *OutboxWorker) Start() {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.logger.Info("Outbox Worker iniciado...")
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-w.ctx.Done():
				w.logger.Info("Outbox Worker encerrado...")
				return
			case <-ticker.C:
				if err := w.processBatch(w.ctx); err != nil {
					if !errors.Is(err, context.Canceled) {
						w.logger.Error("Erro no processamento de lote da outbox", zap.Error(err))
					}
				}
			}
		}
	}()
}

func (w *OutboxWorker) Stop(ctx context.Context) error {
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

func (w *OutboxWorker) processBatch(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, event_type, payload
		FROM outbox
		WHERE published = FALSE
		ORDER BY created_at ASC
		LIMIT 10
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return err
	}

	var processedIDs []string
	var failedIDs []string
	var networkErrorOccurred bool

	for rows.Next() {
		if ctx.Err() != nil {
			rows.Close()
			return ctx.Err()
		}

		var id, aggregateID, eventType string
		var payload []byte
		if err := rows.Scan(&id, &aggregateID, &eventType, &payload); err != nil {
			continue
		}

		if networkErrorOccurred {
			failedIDs = append(failedIDs, id)
			continue
		}

		sendCtx, sendCancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := w.sqs.SendMessage(sendCtx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(w.queueURL),
			MessageBody:            aws.String(string(payload)),
			MessageGroupId:         aws.String(aggregateID),
			MessageDeduplicationId: aws.String(id),
		})
		sendCancel()

		if err != nil {
			if !errors.Is(ctx.Err(), context.Canceled) {
				w.logger.Error("Falha ao publicar evento no SQS", zap.String("id", id), zap.String("eventType", eventType), zap.Error(err))
			}
			failedIDs = append(failedIDs, id)
			networkErrorOccurred = true
			continue
		}
		processedIDs = append(processedIDs, id)
	}
	rows.Close()

	if len(failedIDs) > 0 {
		_, _ = tx.Exec(ctx, "UPDATE outbox SET retry_count = retry_count + 1 WHERE id::text = ANY($1)", failedIDs)
	}

	if len(processedIDs) > 0 {
		_, err = tx.Exec(ctx, "UPDATE outbox SET published = TRUE WHERE id::text = ANY($1)", processedIDs)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
