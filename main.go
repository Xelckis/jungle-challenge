package main

import (
	"cmp"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"jungleChallenge/internal/api/handler"
	"jungleChallenge/internal/api/middleware"
	"jungleChallenge/internal/repository"
	"jungleChallenge/internal/worker"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

//go:embed migrations/000001_init_schema.up.sql
var initSchemaSQL string

func RegisterReferenceWorker(lc fx.Lifecycle, w *worker.ReferenceWorker, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Iniciando Worker de Referencias...")
			w.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Parando Worker de Referencias...")
			return w.Stop(ctx)
		},
	})
}

func ensureQueues(ctx context.Context, client *sqs.Client, logger *zap.Logger) {
	dlqName := "wager-transactions-dlq.fifo"
	dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(dlqName),
		Attributes: map[string]string{
			"FifoQueue": "true",
		},
	})
	if err != nil {
		logger.Warn("Aviso ao verificar/criar fila DLQ SQS", zap.Error(err))
		return
	}

	var dlqArn string
	if dlqOut.QueueUrl != nil {
		attrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: dlqOut.QueueUrl,
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameQueueArn,
			},
		})
		if err == nil && attrOut.Attributes != nil {
			dlqArn = attrOut.Attributes[string(types.QueueAttributeNameQueueArn)]
		}
	}
	if dlqArn == "" {
		dlqArn = "arn:aws:sqs:us-east-1:000000000000:wager-transactions-dlq.fifo"
	}

	mainQueueName := "wager-transactions.fifo"
	redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"3"}`, dlqArn)
	_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(mainQueueName),
		Attributes: map[string]string{
			"FifoQueue":     "true",
			"RedrivePolicy": redrivePolicy,
		},
	})
	if err != nil {
		logger.Warn("Aviso ao verificar/criar fila principal SQS", zap.Error(err))
	} else {
		logger.Info("Filas SQS FIFO inicializadas/verificadas com sucesso.")
	}
}

func NewSQSClient(logger *zap.Logger) (*sqs.Client, error) {
	endpoint := cmp.Or(os.Getenv("AWS_ENDPOINT_URL"), os.Getenv("SQS_ENDPOINT"), "http://127.0.0.1:4566")

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, fmt.Errorf("falha na config AWS: %w", err)
	}

	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	initCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ensureQueues(initCtx, client, logger)

	return client, nil
}

func RegisterOutboxWorker(lc fx.Lifecycle, w *worker.OutboxWorker, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Iniciando Outbox Worker...")
			w.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Parando Outbox Worker...")
			return w.Stop(ctx)
		},
	})
}

func NewDatabase(lc fx.Lifecycle, logger *zap.Logger) (*pgxpool.Pool, error) {
	dbURL := cmp.Or(os.Getenv("DATABASE_URL"), "postgres://jungle_user:jungle_password@127.0.0.1:5432/jungle_db?sslmode=disable")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("falha ao conectar ao banco de dados: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		logger.Warn("Aviso ao pingar banco de dados na inicialização", zap.Error(err))
	} else {
		if _, err := pool.Exec(ctx, initSchemaSQL); err != nil {
			logger.Error("Erro ao aplicar schema inicial no banco de dados", zap.Error(err))
		} else {
			logger.Info("Schema do banco de dados inicializado/verificado com sucesso.")
		}
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logger.Info("Fechando pool de conexões com o banco de dados...")
			pool.Close()
			return nil
		},
	})

	return pool, nil
}

func NewLogger() (*zap.Logger, error) {
	return zap.NewProduction()
}

func NewRouter(pool *pgxpool.Pool, sqsClient *sqs.Client, auth *middleware.AuthMiddleware, walletHandler *handler.WalletHandler, wagerHandler *handler.WagerHandler) *gin.Engine {
	r := gin.Default()

	r.GET("/health/ready", func(c *gin.Context) {
		ctx := c.Request.Context()
		if err := pool.Ping(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "indisponível",
				"error":  "banco de dados indisponível",
			})
			return
		}
		if _, err := sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{}); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "indisponível",
				"error":  "SQS indisponível",
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "pronto"})
	})

	r.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "vivo"})
	})

	// Rotas internas de gerenciamento de carteira
	wallets := r.Group("/wallets")
	{
		wallets.POST("", walletHandler.Create)
		wallets.GET("/:walletId", walletHandler.GetWallet)
		wallets.GET("/:walletId/ledger", walletHandler.GetLedger)
		wallets.POST("/:walletId/reconciliation", walletHandler.Reconcile)
		wallets.GET("/transactions/:transactionId", wagerHandler.GetInternalTransaction)
	}

	// Consulta interna de transação por ID
	r.GET("/wagering/transactions/:transactionId", wagerHandler.GetInternalTransaction)

	// Rotas protegidas por autenticação OIDC / Provedores
	protected := r.Group("")
	protected.Use(auth.RequireProviderId())
	{
		protected.POST("/wagering/transactions", wagerHandler.Process)
		protected.GET("/providers/:providerId/wagering/transactions/:externalTransactionId", wagerHandler.GetExternalTransaction)

		api := protected.Group("/api")
		{
			api.GET("/test-auth", func(c *gin.Context) {
				providerId := c.GetString("providerId")
				c.JSON(http.StatusOK, gin.H{
					"message":    "Autenticado com sucesso!",
					"providerId": providerId,
				})
			})
			api.POST("/wagering/transactions", wagerHandler.Process)
		}
	}

	return r
}

func StartServer(lc fx.Lifecycle, router *gin.Engine, logger *zap.Logger) {
	srv := &http.Server{
		Addr:    ":8081",
		Handler: router,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Iniciando servidor HTTP na porta 8081...")
			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Fatal("Erro no servidor HTTP", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Encerrando servidor HTTP em segurança...")
			return srv.Shutdown(ctx)
		},
	})
}

func RegisterInboxWorker(lc fx.Lifecycle, w *worker.InboxWorker, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Iniciando Inbox Worker...")
			w.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Parando Inbox Worker...")
			return w.Stop(ctx)
		},
	})
}

func main() {
	app := fx.New(
		fx.Provide(
			NewLogger,
			NewDatabase,
			NewSQSClient,
			worker.NewOutboxWorker,
			middleware.NewAuthMiddleware,
			NewRouter,
			handler.NewWagerHandler,
			handler.NewWalletHandler,
			repository.NewWalletRepository,
			worker.NewInboxWorker,
			worker.NewReferenceWorker,
		),
		fx.Invoke(
			StartServer,
			RegisterOutboxWorker,
			RegisterInboxWorker,
			RegisterReferenceWorker,
		),
	)

	app.Run()
}
