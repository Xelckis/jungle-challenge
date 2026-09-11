package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"jungleChallenge/internal/repository"

	"go.uber.org/zap"
)

type ReferenceWorker struct {
	repo   *repository.WalletRepository
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *zap.Logger
}

func NewReferenceWorker(repo *repository.WalletRepository, logger *zap.Logger) *ReferenceWorker {
	return &ReferenceWorker{
		repo:   repo,
		logger: logger,
	}
}

func (w *ReferenceWorker) Start() {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.logger.Info("Iniciando Worker de Referencias Pendentes...")
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-w.ctx.Done():
				w.logger.Info("Worker de Referencias Pendentes encerrado...")
				return
			case <-ticker.C:
				w.processAllPending(w.ctx)
			}
		}
	}()
}

func (w *ReferenceWorker) Stop(ctx context.Context) error {
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

func (w *ReferenceWorker) processAllPending(ctx context.Context) {
	ttl := 24 * time.Hour

	for {
		if ctx.Err() != nil {
			return
		}
		found, err := w.repo.ResolveNextPendingReference(ctx, ttl)
		if err != nil {
			if !errors.Is(ctx.Err(), context.Canceled) {
				w.logger.Error("Erro ao processar referencia pendente", zap.Error(err))
			}
			break
		}
		if !found {
			break
		}
	}
}
