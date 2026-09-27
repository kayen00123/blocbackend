package main

import (
    "bufio"
    "database/sql"
    "fmt"
    "os"
    "strings"

    _ "github.com/lib/pq"
)

func main() {
    loadEnvFile(".env")
    loadEnvFile("../.env")
    dsn := os.Getenv("DATABASE_URL")
    if dsn == "" {
        panic("DATABASE_URL is required")
    }

    db, err := sql.Open("postgres", dsn)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    pairID := strings.TrimSpace(os.Getenv("SETTLEMENT_PAIR_ID"))
    if pairID == "" {
        var ordersBefore, fillsBefore int
        _ = db.QueryRow(`SELECT COUNT(*) FROM dex_orders WHERE network='loadtest' OR pair_id LIKE 'loadtest-%';`).Scan(&ordersBefore)
        _ = db.QueryRow(`SELECT COUNT(*) FROM dex_fills WHERE pair_id LIKE 'loadtest-%';`).Scan(&fillsBefore)
        fmt.Printf("before pair=<unset> orders=%d fills=%d\n", ordersBefore, fillsBefore)

        if _, err := db.Exec(`DELETE FROM dex_fills WHERE pair_id LIKE 'loadtest-%';`); err != nil {
            panic(err)
        }
        if _, err := db.Exec(`DELETE FROM dex_orders WHERE network='loadtest' OR pair_id LIKE 'loadtest-%';`); err != nil {
            panic(err)
        }

        var ordersAfter, fillsAfter int
        _ = db.QueryRow(`SELECT COUNT(*) FROM dex_orders WHERE network='loadtest' OR pair_id LIKE 'loadtest-%';`).Scan(&ordersAfter)
        _ = db.QueryRow(`SELECT COUNT(*) FROM dex_fills WHERE pair_id LIKE 'loadtest-%';`).Scan(&fillsAfter)
        fmt.Printf("after pair=<unset> orders=%d fills=%d\n", ordersAfter, fillsAfter)
        return
    }

    var ordersBefore, fillsBefore int
    _ = db.QueryRow(`SELECT COUNT(*) FROM dex_orders WHERE pair_id = $1 OR pair_id LIKE 'loadtest-%'`, pairID).Scan(&ordersBefore)
    _ = db.QueryRow(`SELECT COUNT(*) FROM dex_fills WHERE pair_id = $1 OR pair_id LIKE 'loadtest-%'`, pairID).Scan(&fillsBefore)
    fmt.Printf("before pair=%s orders=%d fills=%d\n", pairID, ordersBefore, fillsBefore)

    if _, err := db.Exec(`DELETE FROM dex_fills WHERE pair_id = $1 OR pair_id LIKE 'loadtest-%'`, pairID); err != nil {
        panic(err)
    }
    if _, err := db.Exec(`DELETE FROM dex_orders WHERE pair_id = $1 OR pair_id LIKE 'loadtest-%'`, pairID); err != nil {
        panic(err)
    }

    var ordersAfter, fillsAfter int
    _ = db.QueryRow(`SELECT COUNT(*) FROM dex_orders WHERE pair_id = $1 OR pair_id LIKE 'loadtest-%'`, pairID).Scan(&ordersAfter)
    _ = db.QueryRow(`SELECT COUNT(*) FROM dex_fills WHERE pair_id = $1 OR pair_id LIKE 'loadtest-%'`, pairID).Scan(&fillsAfter)
    fmt.Printf("after pair=%s orders=%d fills=%d\n", pairID, ordersAfter, fillsAfter)
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
