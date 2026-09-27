package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	baseMint := flag.String("base-mint", "", "devnet base mint public key")
	quoteMint := flag.String("quote-mint", "", "devnet quote mint public key")
	baseDecimals := flag.Int("base-decimals", 6, "base mint decimals")
	quoteDecimals := flag.Int("quote-decimals", 6, "quote mint decimals")
	price := flag.Float64("price", 1, "initial quote/base price")
	flag.Parse()
	if *baseMint == "" || *quoteMint == "" {
		fatal("base-mint and quote-mint are required")
	}
	if *baseDecimals < 0 || *baseDecimals > 9 || *quoteDecimals < 0 || *quoteDecimals > 9 || *price <= 0 {
		fatal("invalid decimals or price")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		fatal("open database: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fatal("database unavailable: %v", err)
	}

	pairID := fmt.Sprintf("devnet:%s:%s", *baseMint, *quoteMint)
	_, err = db.ExecContext(ctx, `
		INSERT INTO pools (
			address, pool_type, program_id, network, base_mint, base_name, base_symbol, base_decimals,
			quote_mint, quote_name, quote_symbol, quote_decimals, lp_mint,
			pool_base_token_account, pool_quote_token_account, creator, coin_creator,
			pool_index, updated_slot, discovered_at
		) VALUES ($1,'devnet_test','devnet_test','solana',$2,'Devnet Base','DVB',$3,$4,'Devnet Quote','DVQ',$5,
			'devnet-test','devnet-test','devnet-test','devnet-test','devnet-test',0,0,NOW())
		ON CONFLICT (address) DO UPDATE SET
			base_mint=EXCLUDED.base_mint, base_decimals=EXCLUDED.base_decimals,
			quote_mint=EXCLUDED.quote_mint, quote_decimals=EXCLUDED.quote_decimals,
			updated_slot=0, indexed_at=NOW()`,
		pairID, *baseMint, *baseDecimals, *quoteMint, *quoteDecimals)
	if err != nil {
		fatal("register pair: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO latest_prices (pool_address, price, inverse_price, price_change, price_change_percent,
			price_change_direction, fdv_usd, token_price_usd, total_supply, supply_basis,
			base_reserve, quote_reserve, updated_slot, updated_at)
		VALUES ($1,$2,$3,0,0,'flat',0,0,0,'devnet_test',1000000000,1000000000,0,NOW())
		ON CONFLICT (pool_address) DO UPDATE SET price=EXCLUDED.price, inverse_price=EXCLUDED.inverse_price, updated_at=NOW()`,
		pairID, *price, 1 / *price)
	if err != nil {
		fatal("register price: %v", err)
	}

	fmt.Printf("pairId=%s\nbaseMint=%s\nquoteMint=%s\nbaseDecimals=%d\nquoteDecimals=%d\nprice=%g\n", pairID, *baseMint, *quoteMint, *baseDecimals, *quoteDecimals, *price)
}

func fatal(format string, args ...any) {
	fmt.Printf("register error: "+format+"\n", args...)
	os.Exit(1)
}
