package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
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

// Logs a failed key to a file with thread-safe write
func logFailedKey(file *os.File, mu *sync.Mutex, key string) {
	if file == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	file.WriteString(key + "\n")
}

// Copies a single key from source to dest with retry logic and optional error logging
func copyKey(ctx context.Context, source, dest *redis.Client, key string, logger *zap.SugaredLogger, filter *bloom.BloomFilter, errLogFile *os.File, errLogMu *sync.Mutex) {
	if filter != nil && filter.TestString(key) {
		logger.Debugf("Skipping already copied key: %s", key)
		return
	}

	const maxRetries = 3
	const retryDelay = 300 * time.Millisecond

	var val string
	var ttl time.Duration
	var err error

	// Retry DUMP
	for i := 0; i < maxRetries; i++ {
		val, err = source.Dump(ctx, key).Result()
		if err == nil {
			break
		}
		logger.Warnf("DUMP failed for key %s (attempt %d/%d): %v", key, i+1, maxRetries, err)
		time.Sleep(retryDelay)
	}
	if err != nil {
		logger.Errorf("DUMP ultimately failed for key %s after %d attempts", key, maxRetries)
		logFailedKey(errLogFile, errLogMu, key)
		return
	}

	// Retry PTTL
	for i := 0; i < maxRetries; i++ {
		ttl, err = source.PTTL(ctx, key).Result()
		if err == nil {
			break
		}
		logger.Warnf("PTTL failed for key %s (attempt %d/%d): %v", key, i+1, maxRetries, err)
		time.Sleep(retryDelay)
	}
	if err != nil {
		logger.Errorf("PTTL ultimately failed for key %s after %d attempts. Skipping key.", key, maxRetries)
		logFailedKey(errLogFile, errLogMu, key)
		return
	}

	// Retry RESTORE
	for i := 0; i < maxRetries; i++ {
		err = dest.RestoreReplace(ctx, key, ttl, val).Err()
		if err == nil {
			break
		}
		logger.Warnf("RESTORE failed for key %s (attempt %d/%d): %v", key, i+1, maxRetries, err)
		time.Sleep(retryDelay)
	}
	if err != nil {
		logger.Errorf("RESTORE ultimately failed for key %s after %d attempts", key, maxRetries)
		logFailedKey(errLogFile, errLogMu, key)
		return
	}

	logger.Debugf("Copied key: %s", key)
	if filter != nil {
		filter.AddString(key)
	}
}

// Reprocesses a list of keys from file
func reprocessKeys(ctx context.Context, source, dest *redis.Client, path string, logger *zap.SugaredLogger, errLogFile *os.File) {
	file, err := os.Open(path)
	if err != nil {
		logger.Fatalf("Failed to open retry input file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var mu sync.Mutex
	for scanner.Scan() {
		key := scanner.Text()
		if key != "" {
			copyKey(ctx, source, dest, key, logger, nil, errLogFile, &mu)
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Errorf("Error reading retry file: %v", err)
	}
	logger.Info("Reprocessing complete!")
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

func migrateKeys(ctx context.Context, source, dest *redis.Client, batchSize, workers, sleepMs int, logger *zap.SugaredLogger, filter *bloom.BloomFilter, errLogFile *os.File) {
	var cursor uint64
	var wg sync.WaitGroup
	var errLogMu sync.Mutex
	keyChan := make(chan string, batchSize*workers)

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
					copyKey(ctx, source, dest, key, logger, filter, errLogFile, &errLogMu)
					if sleepMs > 0 {
						time.Sleep(time.Duration(sleepMs) * time.Millisecond)
					}
				}
			}
		}(i)
	}

SCAN:
	for {
		select {
		case <-ctx.Done():
			break SCAN
		default:
			var keys []string
			var nextCursor uint64
			var err error
			for i := 0; i < 3; i++ {
				keys, nextCursor, err = source.Scan(ctx, cursor, "*", int64(batchSize)).Result()
				if err != nil {
					if i == 2 {
						logger.Fatalf("SCAN ultimately failed after %d attempts: %v", i+1, err)
					}
					logger.Warnf("Retrying (%d/3) SCAN error: %v", (i + 1), err)
					time.Sleep(1 * time.Second)
					continue
				} else {
					break
				}
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
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, _ := cfg.Build()
	defer logger.Sync()
	sugar := logger.Sugar()

	sourceAddr := flag.String("source", "", "Source Redis address (host:port)")
	destAddr := flag.String("dest", "", "Destination Redis address (host:port)")
	mode := flag.String("mode", "count", "Mode: 'count', 'migrate', or 'reprocess'")
	workers := flag.Int("workers", 10, "Number of concurrent workers for migration")
	batchSize := flag.Int("batch", 100, "SCAN batch size")
	sleepMs := flag.Int("sleep", 0, "Delay (ms) between copying keys (per worker)")
	resumeBloomFile := flag.String("resume-bloom-file", "resume.bloom", "Path to Bloom filter file for resume support")
	estKeys := flag.Uint("bloom-estimate", 1000000, "Estimated number of keys for Bloom filter")
	fpRate := flag.Float64("bloom-fpr", 0.0001, "False positive rate for Bloom filter")
	errorKeyLog := flag.String("error-key-log", "", "Path to file where failed keys will be logged")
	retryInput := flag.String("retry-input", "", "Path to file with keys to retry")
	retryOutput := flag.String("retry-output", "", "Path to file to log keys that still fail after retry")
	flag.Parse()

	if *sourceAddr == "" || *destAddr == "" {
		sugar.Fatal("Both --source and --dest are required")
	}

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

	var bloomFilter *bloom.BloomFilter
	if *mode == "migrate" {
		if file, err := os.Open(*resumeBloomFile); err == nil {
			defer file.Close()
			bloomFilter = bloom.New(*estKeys, 5)
			_, err := bloomFilter.ReadFrom(file)
			if err != nil {
				sugar.Fatalf("Failed to load Bloom filter: %v", err)
			}
			sugar.Infof("Loaded Bloom filter from %s", *resumeBloomFile)
		} else {
			bloomFilter = bloom.NewWithEstimates(*estKeys, *fpRate)
			sugar.Infof("Initialized new Bloom filter with capacity %d", *estKeys)
		}
		defer func() {
			file, err := os.Create(*resumeBloomFile)
			if err != nil {
				sugar.Errorf("Failed to save Bloom filter: %v", err)
				return
			}
			defer file.Close()
			if _, err := bloomFilter.WriteTo(file); err != nil {
				sugar.Errorf("Failed to write Bloom filter: %v", err)
			} else {
				sugar.Infof("Saved Bloom filter to %s", *resumeBloomFile)
			}
		}()
	}

	var errLogFile *os.File
	if *errorKeyLog != "" {
		var err error
		errLogFile, err = os.Create(*errorKeyLog)
		if err != nil {
			sugar.Fatalf("Failed to create error log file: %v", err)
		}
		defer errLogFile.Close()
	}

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
		migrateKeys(ctx, source, dest, *batchSize, *workers, *sleepMs, sugar, bloomFilter, errLogFile)
		cancelCountLogger()

	case "reprocess":
		if *retryInput == "" {
			sugar.Fatal("--retry-input is required in reprocess mode")
		}
		var retryLogFile *os.File
		if *retryOutput != "" {
			var err error
			retryLogFile, err = os.Create(*retryOutput)
			if err != nil {
				sugar.Fatalf("Failed to create retry output file: %v", err)
			}
			defer retryLogFile.Close()
		}
		reprocessKeys(ctx, source, dest, *retryInput, sugar, retryLogFile)

	default:
		sugar.Fatalf("Unknown mode: %s (use 'count', 'migrate', or 'reprocess')", *mode)
	}
}
