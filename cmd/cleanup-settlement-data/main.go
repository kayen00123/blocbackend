package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	loadEnvFile(".env")
	loadEnvFile("../.env")
	apply := flag.Bool("apply", false, "delete the reported candidates")
	flag.Parse()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		fatal("open database: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fatal("database is unavailable: %v", err)
	}

	var totalFills, unsettledFills, testOrders, staleOrders, deletableOrders int64
	queries := []struct {
		name  string
		query string
		out   *int64
	}{
		{"fills_total", `SELECT COUNT(*) FROM dex_fills`, &totalFills},
		{"fills_unsettled", `SELECT COUNT(*) FROM dex_fills WHERE settlement_status <> 'confirmed'`, &unsettledFills},
		{"orders_test", `SELECT COUNT(*) FROM dex_orders WHERE network='loadtest' OR pair_id LIKE 'loadtest-%'`, &testOrders},
		{"orders_stale_terminal", `SELECT COUNT(*) FROM dex_orders o WHERE o.status IN ('expired','cancelled') AND NOT EXISTS (SELECT 1 FROM dex_fills f WHERE f.settlement_status='confirmed' AND (f.maker_order_id=o.id OR f.taker_order_id=o.id))`, &staleOrders},
		{"orders_deletable_union", `SELECT COUNT(*) FROM dex_orders o WHERE (o.network='loadtest' OR o.pair_id LIKE 'loadtest-%' OR o.status IN ('expired','cancelled')) AND NOT EXISTS (SELECT 1 FROM dex_fills f WHERE f.settlement_status='confirmed' AND (f.maker_order_id=o.id OR f.taker_order_id=o.id))`, &deletableOrders},
	}
	for _, item := range queries {
		if err := db.QueryRowContext(ctx, item.query).Scan(item.out); err != nil {
			fatal("%s: %v", item.name, err)
		}
		fmt.Printf("%s=%d\n", item.name, *item.out)
	}
	rows, err := db.QueryContext(ctx, `SELECT id, settlement_status, COALESCE(last_error, '') FROM dex_fills WHERE settlement_status <> 'confirmed' ORDER BY id`)
	if err != nil {
		fatal("list unsettled fills: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var status, lastError string
		if err := rows.Scan(&id, &status, &lastError); err != nil {
			fatal("decode unsettled fill: %v", err)
		}
		fmt.Printf("unsettled_fill id=%d status=%s error=%q\n", id, status, lastError)
	}

	if !*apply {
		fmt.Println("dry-run only; rerun with -apply to delete unsettled fills and eligible test/stale orders")
		return
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fatal("begin cleanup transaction: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE cleanup_order_ids (id BIGINT PRIMARY KEY) ON COMMIT DROP`); err != nil {
		fatal("create cleanup staging table: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cleanup_order_ids (id)
		SELECT maker_order_id FROM dex_fills WHERE settlement_status <> 'confirmed'
		UNION
		SELECT taker_order_id FROM dex_fills WHERE settlement_status <> 'confirmed'
		UNION
		SELECT id FROM dex_orders WHERE network='loadtest' OR pair_id LIKE 'loadtest-%' OR status IN ('expired','cancelled')`); err != nil {
		fatal("stage cleanup orders: %v", err)
	}

	fillResult, err := tx.ExecContext(ctx, `DELETE FROM dex_fills WHERE settlement_status <> 'confirmed'`)
	if err != nil {
		fatal("delete unsettled fills: %v", err)
	}
	deletedFills, err := fillResult.RowsAffected()
	if err != nil {
		fatal("count deleted fills: %v", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dex_fills WHERE settlement_status <> 'confirmed'`).Scan(new(int64)); err != nil {
		fatal("verify fill deletion: %v", err)
	}
	orderResult, err := tx.ExecContext(ctx, `DELETE FROM dex_orders o WHERE o.id IN (SELECT id FROM cleanup_order_ids) AND NOT EXISTS (SELECT 1 FROM dex_fills f WHERE f.settlement_status='confirmed' AND (f.maker_order_id=o.id OR f.taker_order_id=o.id))`)
	if err != nil {
		fatal("delete stale orders: %v", err)
	}
	deletedOrders, err := orderResult.RowsAffected()
	if err != nil {
		fatal("count deleted orders: %v", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dex_orders o WHERE (o.network='loadtest' OR o.pair_id LIKE 'loadtest-%' OR o.status IN ('expired','cancelled')) AND NOT EXISTS (SELECT 1 FROM dex_fills f WHERE f.maker_order_id=o.id OR f.taker_order_id=o.id)`).Scan(new(int64)); err != nil {
		fatal("verify order deletion: %v", err)
	}
	if err := tx.Commit(); err != nil {
		fatal("commit cleanup: %v", err)
	}
	fmt.Printf("cleanup committed: deleted fills=%d orders=%d; confirmed fills and their orders were preserved\n", deletedFills, deletedOrders)
}

func loadEnvFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), "\"")
		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
}

func fatal(format string, args ...any) {
	fmt.Printf("cleanup error: "+format+"\n", args...)
	os.Exit(1)
}
