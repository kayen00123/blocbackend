package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		panic("DATABASE_URL is required")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var beforeOrders, beforeFills int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dex_orders WHERE network='loadtest' OR pair_id LIKE 'loadtest-%';`).Scan(&beforeOrders); err != nil {
		panic(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM dex_fills WHERE pair_id LIKE 'loadtest-%';`).Scan(&beforeFills); err != nil {
		panic(err)
	}
	fmt.Printf("before orders=%d fills=%d\n", beforeOrders, beforeFills)

	if _, err := db.Exec(`DELETE FROM dex_fills WHERE pair_id LIKE 'loadtest-%';`); err != nil {
		panic(err)
	}
	if _, err := db.Exec(`DELETE FROM dex_orders WHERE network='loadtest' OR pair_id LIKE 'loadtest-%';`); err != nil {
		panic(err)
	}

	if err := db.QueryRow(`SELECT COUNT(*) FROM dex_orders WHERE network='loadtest' OR pair_id LIKE 'loadtest-%';`).Scan(&beforeOrders); err != nil {
		panic(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM dex_fills WHERE pair_id LIKE 'loadtest-%';`).Scan(&beforeFills); err != nil {
		panic(err)
	}
	fmt.Printf("after orders=%d fills=%d\n", beforeOrders, beforeFills)
}
