package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type notificationsTopicMsg struct {
	OrderID        int `json:"order_id"`
	PreviousStatus int `json:"previous_status"`
	Status         int `json:"status"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	logger, err := newLogger(os.Getenv("LOG_LEVEL"))
	if err != nil {
		log.Fatal(err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER_HOST")},
		Topic:    os.Getenv("KAFKA_TOPIC"),
		MaxBytes: 10e6, // 10MB4
		GroupID:  os.Getenv("KAFKA_GROUP_ID"),
	})

	// Set up a channel to receive OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("Received shutdown signal")
		cancel()
		if err := reader.Close(); err != nil {
			logger.Error("closing kafka reader error", zap.Error(err))
		}
	}()

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			logger.Error("reading message error", zap.Error(err))
			continue
		}

		var msg notificationsTopicMsg

		if err := json.Unmarshal(m.Value, &msg); err != nil {
			logger.Error("failed to unmarshal message", zap.Error(err), zap.ByteString("data", m.Value))
			continue
		}

		if err := reader.CommitMessages(ctx, m); err != nil {
			logger.Error("commit message error", zap.Error(err))
			continue
		}

		logger.Info("message processed", zap.ByteString("data", m.Value))
	}
}

func newLogger(level string) (*zap.Logger, error) {
	atomicLogLevel, err := zap.ParseAtomicLevel(level)
	if err != nil {
		return nil, err
	}

	atom := zap.NewAtomicLevelAt(atomicLogLevel.Level())
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	return zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			atom,
		),
		zap.WithCaller(true),
		zap.Fields(zap.String("app", os.Getenv("APP_ID"))),
		zap.AddStacktrace(zap.ErrorLevel),
	), nil
}
