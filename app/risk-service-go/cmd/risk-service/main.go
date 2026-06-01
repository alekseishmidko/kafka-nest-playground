package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kafka-playground/risk-service-go/internal/app"
	"github.com/kafka-playground/risk-service-go/internal/config"
	"github.com/kafka-playground/risk-service-go/internal/risk"
	"github.com/kafka-playground/risk-service-go/internal/schemaregistry"
)

func main() {
	// Загружаем конфиг из env-файлов сервиса и реальных переменных окружения.
	cfg, err := config.Load(".")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := newLogger(cfg.LogLevel)
	logger.Info("starting service",
		"service", "risk-service-go",
		"environment", cfg.AppEnv,
		"host", cfg.Host,
		"port", cfg.Port,
		"kafkaBrokers", cfg.KafkaBrokers,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Собираем зависимости явно: в Go часто передают их через конструкторы, а не через DI-контейнер.
	registryClient := schemaregistry.NewClient(cfg.SchemaRegistryURL, http.DefaultClient)
	scorer := risk.NewScorer(cfg.RiskScoreIterations, cfg.RiskScoreThreshold)
	service, err := app.NewService(cfg, logger, registryClient, scorer)
	if err != nil {
		logger.Error("failed to initialize service", "error", err)
		os.Exit(1)
	}
	defer service.Close()

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddress(),
		Handler:           healthHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errs := make(chan error, 2)
	go func() {
		// Health HTTP-сервер работает параллельно с Kafka consumer loop.
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()
	go func() {
		// Основная работа сервиса: читать Kafka, считать risk score, публиковать результат.
		errs <- service.Run(ctx)
	}()

	// Ждем либо сигнал завершения, либо ошибку из одной из фоновых goroutine.
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errs:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("service stopped with error", "error", err)
			stop()
		}
	}

	// Даем HTTP-серверу немного времени корректно закрыть активные запросы.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http server shutdown failed", "error", err)
	}
	logger.Info("service stopped", "service", "risk-service-go")
}

func newLogger(level string) *slog.Logger {
	// slog входит в стандартную библиотеку Go и пишет структурированные JSON-логи.
	var slogLevel slog.Level
	switch level {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel}))
}

func healthHandler() http.Handler {
	// ServeMux - стандартный HTTP-router из net/http.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	return mux
}
