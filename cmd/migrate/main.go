package main

import (
	"context"
	"crypto/tls"
	"flag"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true, // only for self-signed certs or testing
		},
	})
}

func getDBSize(ctx context.Context, client *redis.Client, label string, logger *zap.SugaredLogger) {
	size, err := client.DBSize(ctx).Result()
	if err != nil {
		logger.Errorf("[%s] Failed to get DB size: %v", label, err)
		return
	}
	logger.Infof("[%s] DB size: %d keys", label, size)
}

func copyKey(ctx context.Context, source, dest *redis.Client, key string, logger *zap.SugaredLogger) {
	val, err := source.Dump(ctx, key).Result()
	if err != nil {
		logger.Warnf("DUMP failed for key %s: %v", key, err)
		return
	}

	ttl, err := source.PTTL(ctx, key).Result()
	if err != nil || ttl < 0 {
		ttl = 0
	}

	err = dest.RestoreReplace(ctx, key, ttl, val).Err()
	if err != nil {
		logger.Warnf("RESTORE failed for key %s: %v", key, err)
	} else {
		logger.Debugf("Copied key: %s", key)
	}
}

func migrateKeys(ctx context.Context, source, dest *redis.Client, batchSize, workers, sleepMs int, logger *zap.SugaredLogger) {
	var cursor uint64
	var wg sync.WaitGroup
	keyChan := make(chan string, batchSize*workers)

	// Worker pool
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for key := range keyChan {
				copyKey(ctx, source, dest, key, logger)
				if sleepMs > 0 {
					time.Sleep(time.Duration(sleepMs) * time.Millisecond)
				}
			}
		}(i)
	}

	// Producer: scan and enqueue keys
	for {
		keys, nextCursor, err := source.Scan(ctx, cursor, "*", int64(batchSize)).Result()
		if err != nil {
			logger.Fatalf("Scan error: %v", err)
		}

		for _, key := range keys {
			keyChan <- key
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	close(keyChan)
	wg.Wait()
	logger.Info("Migration complete!")
}

func startKeyCountLogger(ctx context.Context, dest *redis.Client, logger *zap.SugaredLogger) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				size, err := dest.DBSize(context.Background()).Result()
				if err != nil {
					logger.Warnf("Failed to get dest DB size: %v", err)
				} else {
					logger.Infof("[DEST] Key count: %d", size)
				}
			}
		}
	}()

	return cancel
}

func main() {
	// Init logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	sugar := logger.Sugar()

	// Flags
	sourceAddr := flag.String("source", "", "Source Redis address (host:port)")
	destAddr := flag.String("dest", "", "Destination Redis address (host:port)")
	mode := flag.String("mode", "count", "Mode: 'count' or 'migrate'")
	workers := flag.Int("workers", 10, "Number of concurrent workers for migration")
	batchSize := flag.Int("batch", 100, "SCAN batch size")
	sleepMs := flag.Int("sleep", 0, "Delay (ms) between copying keys (per worker)")

	flag.Parse()

	if *sourceAddr == "" || *destAddr == "" {
		sugar.Fatal("Both --source and --dest are required")
	}

	ctx := context.Background()
	source := newRedisClient(*sourceAddr)
	dest := newRedisClient(*destAddr)

	switch *mode {
	case "count":
		getDBSize(ctx, source, "SOURCE", sugar)
		getDBSize(ctx, dest, "DEST", sugar)
	case "migrate":
		sugar.Infof("Starting migration with %d workers, batch size %d, sleep %dms", *workers, *batchSize, *sleepMs)
		getDBSize(ctx, source, "SOURCE", sugar)
		getDBSize(ctx, dest, "DEST", sugar)
		cancelCountLogger := startKeyCountLogger(ctx, dest, sugar)
		migrateKeys(ctx, source, dest, *batchSize, *workers, *sleepMs, sugar)
		cancelCountLogger()
		getDBSize(ctx, source, "SOURCE", sugar)
		getDBSize(ctx, dest, "DEST", sugar)
	default:
		sugar.Fatalf("Unknown mode: %s (use 'count' or 'migrate')", *mode)
	}
}
