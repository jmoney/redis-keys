package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Create a Redis client with optional TLS
func newRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true, // use only for testing/self-signed certs
		},
	})
}

// Generic function to return DB size
func getDBSize(ctx context.Context, client *redis.Client) (int64, error) {
	return client.DBSize(ctx).Result()
}

// Copies a single key from source to dest
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

// Kicks off a goroutine to log destination DB size every 2 seconds
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
				if size, err := getDBSize(context.Background(), dest); err != nil {
					logger.Warnf("Failed to get DEST DB size: %v", err)
				} else {
					logger.Infof("[DEST] Key count: %d", size)
				}
			}
		}
	}()

	return cancel
}

// Performs migration from source to dest with concurrency
func migrateKeys(ctx context.Context, source, dest *redis.Client, batchSize, workers, sleepMs int, logger *zap.SugaredLogger) {
	var cursor uint64
	var wg sync.WaitGroup
	keyChan := make(chan string, batchSize*workers)

	// Worker pool
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case key, ok := <-keyChan:
					if !ok {
						return
					}
					copyKey(ctx, source, dest, key, logger)
					if sleepMs > 0 {
						time.Sleep(time.Duration(sleepMs) * time.Millisecond)
					}
				}
			}
		}(i)
	}

	// SCAN loop to feed keys
SCAN:
	for {
		select {
		case <-ctx.Done():
			break SCAN
		default:
			keys, nextCursor, err := source.Scan(ctx, cursor, "*", int64(batchSize)).Result()
			if err != nil {
				logger.Fatalf("SCAN error: %v", err)
			}

			for _, key := range keys {
				select {
				case <-ctx.Done():
					break SCAN
				case keyChan <- key:
				}
			}

			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	}

	close(keyChan)
	wg.Wait()
	logger.Info("Migration complete!")
}

func main() {
	// Setup colorized logger
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, _ := cfg.Build()
	defer logger.Sync()
	sugar := logger.Sugar()

	// CLI flags
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

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		sugar.Warn("Shutdown signal received, cleaning up...")
		cancel()
	}()

	source := newRedisClient(*sourceAddr)
	dest := newRedisClient(*destAddr)

	switch *mode {
	case "count":
		if size, err := getDBSize(ctx, source); err != nil {
			sugar.Errorf("Failed to get SOURCE DB size: %v", err)
		} else {
			sugar.Infof("[SOURCE] DB size: %d keys", size)
		}

		if size, err := getDBSize(ctx, dest); err != nil {
			sugar.Errorf("Failed to get DEST DB size: %v", err)
		} else {
			sugar.Infof("[DEST] DB size: %d keys", size)
		}

	case "migrate":
		sugar.Infof("Starting migration with %d workers, batch size %d, sleep %dms", *workers, *batchSize, *sleepMs)
		cancelCountLogger := startKeyCountLogger(ctx, dest, sugar)
		migrateKeys(ctx, source, dest, *batchSize, *workers, *sleepMs, sugar)
		cancelCountLogger()

	default:
		sugar.Fatalf("Unknown mode: %s (use 'count' or 'migrate')", *mode)
	}
}
