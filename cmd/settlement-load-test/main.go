package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	count := flag.Int("count", 1000, "number of synthetic fills to create")
	workers := flag.Int("workers", 32, "number of concurrent claim workers")
	cleanup := flag.Bool("cleanup", true, "delete generated rows after the test")
	flag.Parse()
	if *count < 1 || *workers < 1 {
		fatal("count and workers must be positive")
	}
	if os.Getenv("SETTLEMENT_LOAD_TEST") != "true" {
		fatal("refusing to run: set SETTLEMENT_LOAD_TEST=true")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	apiURL := os.Getenv("SETTLEMENT_API_URL")
	apiKey := os.Getenv("SETTLEMENT_API_KEY")
	if databaseURL == "" || apiURL == "" || apiKey == "" {
		fatal("DATABASE_URL, SETTLEMENT_API_URL, and SETTLEMENT_API_KEY are required")
	}
	if os.Getenv("SETTLEMENT_LIVE") == "true" {
		fatal("refusing to run while SETTLEMENT_LIVE=true")
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		fatal("open database: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fatal("database is unavailable: %v", err)
	}

	prefix := fmt.Sprintf("loadtest-%d", time.Now().UnixNano())
	ids, err := seed(ctx, db, prefix, *count)
	if err != nil {
		fatal("seed synthetic fills: %v", err)
	}
	if *cleanup {
		defer cleanupRows(context.Background(), db, prefix)
	}

	var claimed atomic.Int64
	var conflicts atomic.Int64
	var failures atomic.Int64
	var firstFailure atomic.Bool
	jobs := make(chan int64)
	start := time.Now()
	var group sync.WaitGroup
	for worker := 0; worker < *workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			owner := fmt.Sprintf("loadtest-worker-%d", worker)
			for id := range jobs {
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/api/settlement/fills/"+strconv.FormatInt(id, 10)+"/claim", nil)
				req.Header.Set("X-Settlement-Key", apiKey)
				req.Header.Set("X-Settlement-Worker", owner)
				resp, requestErr := client.Do(req)
				if requestErr != nil {
					failures.Add(1)
					if firstFailure.CompareAndSwap(false, true) {
						fmt.Printf("first failure worker=%d fill=%d error=%v\n", worker, id, requestErr)
					}
					continue
				}
				status := resp.StatusCode
				if status != http.StatusOK {
					if firstFailure.CompareAndSwap(false, true) {
						fmt.Printf("first failure worker=%d fill=%d status=%d\n", worker, id, status)
					}
				}
				_ = resp.Body.Close()
				switch status {
				case http.StatusOK:
					claimed.Add(1)
				case http.StatusConflict:
					conflicts.Add(1)
				default:
					failures.Add(1)
				}
			}
		}(worker)
	}
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	group.Wait()

	elapsed := time.Since(start)
	fmt.Printf("load-test fills=%d workers=%d claimed=%d conflicts=%d failures=%d elapsed=%s throughput=%.2f claims/s\n", *count, *workers, claimed.Load(), conflicts.Load(), failures.Load(), elapsed.Round(time.Millisecond), float64(claimed.Load())/elapsed.Seconds())
	if claimed.Load() != int64(*count) || conflicts.Load() != 0 || failures.Load() != 0 {
		os.Exit(2)
	}
}

func seed(ctx context.Context, db *sql.DB, prefix string, count int) ([]int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ids := make([]int64, 0, count)
	for index := 0; index < count; index++ {
		var makerID, takerID, fillID int64
		expiration := time.Now().Add(24 * time.Hour).Unix()
		order := `INSERT INTO dex_orders (order_hash,maker,pair_id,network,side,order_type,price,amount,amount_in,amount_out_min,token_in,token_out,receiver,signature,expiration,nonce,salt,status) VALUES ($1,$2,$3,'loadtest',$4,'limit','1','1','1','1',$5,$6,$2,$7,$8,$9,$9,'open') RETURNING id`
		maker := "0x0000000000000000000000000000000000000001"
		taker := "0x0000000000000000000000000000000000000002"
		if err := tx.QueryRowContext(ctx, order, fmt.Sprintf("%s-maker-%d", prefix, index), maker, prefix, "sell", maker, taker, "0x", expiration, index+1).Scan(&makerID); err != nil {
			return nil, err
		}
		if err := tx.QueryRowContext(ctx, order, fmt.Sprintf("%s-taker-%d", prefix, index), taker, prefix, "buy", taker, maker, "0x", expiration, count+index+1).Scan(&takerID); err != nil {
			return nil, err
		}
		if err := tx.QueryRowContext(ctx, `INSERT INTO dex_fills (maker_order_id,taker_order_id,pair_id,price,amount,amount_quote,maker,taker) VALUES ($1,$2,$3,'1','1','1',$4,$5) RETURNING id`, makerID, takerID, prefix, maker, taker).Scan(&fillID); err != nil {
			return nil, err
		}
		ids = append(ids, fillID)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func cleanupRows(ctx context.Context, db *sql.DB, prefix string) {
	_, _ = db.ExecContext(ctx, `DELETE FROM dex_fills WHERE pair_id=$1`, prefix)
	_, _ = db.ExecContext(ctx, `DELETE FROM dex_orders WHERE pair_id=$1`, prefix)
}

func fatal(format string, args ...any) {
	fmt.Printf("load-test error: "+format+"\n", args...)
	os.Exit(1)
}
