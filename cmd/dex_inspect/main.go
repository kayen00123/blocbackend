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

	rows, err := db.Query(`SELECT network, pair_id, COUNT(*) AS c FROM dex_orders GROUP BY network, pair_id ORDER BY c DESC LIMIT 20;`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	fmt.Println("ORDER SUMMARY")
	for rows.Next() {
		var network, pairID string
		var c int
		if err := rows.Scan(&network, &pairID, &c); err != nil {
			panic(err)
		}
		fmt.Printf("%s | %s | %d\n", network, pairID, c)
	}

	rows2, err := db.Query(`SELECT network, pair_id, COUNT(*) AS c FROM dex_fills GROUP BY network, pair_id ORDER BY c DESC LIMIT 20;`)
	if err != nil {
		panic(err)
	}
	defer rows2.Close()
	fmt.Println("FILL SUMMARY")
	for rows2.Next() {
		var network, pairID string
		var c int
		if err := rows2.Scan(&network, &pairID, &c); err != nil {
			panic(err)
		}
		fmt.Printf("%s | %s | %d\n", network, pairID, c)
	}
}
