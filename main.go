package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lib/pq"
)

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

type Pair struct {
	ID                  string   `json:"id"`
	PoolType            string   `json:"poolType"`
	Network             string   `json:"network"`
	Symbol              string   `json:"symbol"`
	BaseSymbol          *string  `json:"baseSymbol,omitempty"`
	QuoteSymbol         *string  `json:"quoteSymbol,omitempty"`
	BaseToken           string   `json:"baseToken"`
	QuoteToken          string   `json:"quoteToken"`
	BaseLogoURL         *string  `json:"baseLogoUrl,omitempty"`
	QuoteLogoURL        *string  `json:"quoteLogoUrl,omitempty"`
	Price               *float64 `json:"price,omitempty"`
	InversePrice        *float64 `json:"inversePrice,omitempty"`
	TokenPriceUSD       *float64 `json:"tokenPriceUsd,omitempty"`
	PriceChange         *float64 `json:"priceChangePercent,omitempty"`
	High24h             *float64 `json:"high24h,omitempty"`
	Low24h              *float64 `json:"low24h,omitempty"`
	MarketPrice         *float64 `json:"marketPrice,omitempty"`
	MarketChange        *float64 `json:"marketPriceChangePercent,omitempty"`
	Liquidity           *float64 `json:"liquidity,omitempty"`
	LiquidityUSD        *float64 `json:"liquidityUsd,omitempty"`
	Volume24h           *float64 `json:"volume24h,omitempty"`
	Volume24hUSD        *float64 `json:"volume24hUsd,omitempty"`
	TransactionCount1h  *float64 `json:"transactionCount1h,omitempty"`
	TransactionCount24h *float64 `json:"transactionCount24h,omitempty"`
	UpdatedAt           *string  `json:"updatedAt,omitempty"`
}

type PairCandle struct {
	Timestamp int64   `json:"timestamp"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Turnover  float64 `json:"turnover"`
}

type CreateOrderRequest struct {
	OrderHash        string `json:"orderHash"`
	Maker            string `json:"maker"`
	PairID           string `json:"pairId"`
	Network          string `json:"network"`
	Side             string `json:"side"`
	OrderType        string `json:"orderType"`
	ExecutionType    string `json:"executionType"`
	Price            string `json:"price"`
	TriggerPrice     string `json:"triggerPrice"`
	Amount           string `json:"amount"`
	AmountIn         string `json:"amountIn"`
	AmountOutMin     string `json:"amountOutMin"`
	TokenIn          string `json:"tokenIn"`
	TokenOut         string `json:"tokenOut"`
	TokenInAccount   string `json:"tokenInAccount,omitempty"`
	TokenOutAccount  string `json:"tokenOutAccount,omitempty"`
	Receiver         string `json:"receiver"`
	Signature        string `json:"signature"`
	Expiration       int64  `json:"expiration"`
	Nonce            uint64 `json:"nonce"`
	Salt             uint64 `json:"salt"`
	LadderLevels     int    `json:"ladderLevels"`
	LadderPriceStart string `json:"ladderPriceStart"`
	LadderPriceEnd   string `json:"ladderPriceEnd"`
	LadderAmount     string `json:"ladderAmount"`
	PostOnly         bool   `json:"postOnly"`
}

type StoredOrder struct {
	ID                int64  `json:"id"`
	OrderHash         string `json:"orderHash"`
	Maker             string `json:"maker"`
	PairID            string `json:"pairId"`
	Network           string `json:"network"`
	Side              string `json:"side"`
	OrderType         string `json:"orderType"`
	Price             string `json:"price"`
	TriggerPrice      string `json:"triggerPrice,omitempty"`
	Amount            string `json:"amount"`
	FilledAmount      string `json:"filledAmount"`
	AmountIn          string `json:"amountIn"`
	AmountOutMin      string `json:"amountOutMin"`
	TokenIn           string `json:"tokenIn"`
	TokenOut          string `json:"tokenOut"`
	TokenInAccount    string `json:"tokenInAccount,omitempty"`
	TokenOutAccount   string `json:"tokenOutAccount,omitempty"`
	Receiver          string `json:"receiver"`
	Signature         string `json:"signature"`
	Expiration        int64  `json:"expiration"`
	Nonce             uint64 `json:"nonce"`
	Salt              uint64 `json:"salt"`
	Status            string `json:"status"`
	CreatedAt         string `json:"createdAt"`
	UpdatedAt         string `json:"updatedAt,omitempty"`
	LadderLevels      int    `json:"ladderLevels,omitempty"`
	LadderPriceStart  string `json:"ladderPriceStart,omitempty"`
	LadderPriceEnd    string `json:"ladderPriceEnd,omitempty"`
	LadderLevel       int    `json:"ladderLevel,omitempty"`
	LadderParentHash  string `json:"ladderParentHash,omitempty"`
	LadderTotalAmount string `json:"ladderTotalAmount,omitempty"`
	IsLadder          bool   `json:"isLadder,omitempty"`
	PostOnly          bool   `json:"postOnly,omitempty"`
	FillPrice         string `json:"fillPrice,omitempty"`
}

type StoredFill struct {
	ID                 int64  `json:"id"`
	MakerOrderID       int64  `json:"makerOrderId"`
	TakerOrderID       int64  `json:"takerOrderId"`
	PairID             string `json:"pairId"`
	Network            string `json:"network"`
	Price              string `json:"price"`
	Amount             string `json:"amount"`
	AmountQuote        string `json:"amountQuote"`
	Maker              string `json:"maker"`
	Taker              string `json:"taker"`
	Side               string `json:"side"`
	CreatedAt          string `json:"createdAt"`
	SettlementStatus   string `json:"settlementStatus"`
	SettlementAttempts int    `json:"settlementAttempts"`
	LastAttemptedAt    string `json:"lastAttemptedAt,omitempty"`
	TxHash             string `json:"txHash,omitempty"`
	TxHashBuy          string `json:"txHashBuy,omitempty"`
	TxHashSell         string `json:"txHashSell,omitempty"`
	LastError          string `json:"lastError,omitempty"`
	QuoteTokenSymbol   string `json:"quoteTokenSymbol,omitempty"`
	QuoteTokenAddress  string `json:"quoteTokenAddress,omitempty"`
}

type SettlementPayload struct {
	Fill      StoredFill  `json:"fill"`
	BuyOrder  StoredOrder `json:"buyOrder"`
	SellOrder StoredOrder `json:"sellOrder"`
}

func decodeBase58(value string) ([]byte, error) {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	result := []byte{}
	for _, char := range strings.TrimSpace(value) {
		index := strings.IndexRune(alphabet, char)
		if index < 0 {
			return nil, fmt.Errorf("invalid base58 character")
		}
		carry := index
		for i := len(result) - 1; i >= 0; i-- {
			carry += int(result[i]) * 58
			result[i] = byte(carry % 256)
			carry /= 256
		}
		for carry > 0 {
			result = append([]byte{byte(carry % 256)}, result...)
			carry /= 256
		}
	}
	for _, char := range strings.TrimSpace(value) {
		if char != '1' {
			break
		}
		result = append([]byte{0}, result...)
	}
	return result, nil
}

func canonicalSolanaOrderBody(order CreateOrderRequest) []byte {
	message := map[string]interface{}{
		"amount":          order.Amount,
		"amountIn":        order.AmountIn,
		"amountOutMin":    order.AmountOutMin,
		"expiration":      order.Expiration,
		"maker":           order.Maker,
		"network":         "solana",
		"nonce":           order.Nonce,
		"pairId":          order.PairID,
		"price":           order.Price,
		"salt":            order.Salt,
		"side":            order.Side,
		"tokenIn":         order.TokenIn,
		"tokenInAccount":  order.TokenInAccount,
		"tokenOut":        order.TokenOut,
		"tokenOutAccount": order.TokenOutAccount,
	}
	body, _ := json.Marshal(message)
	return body
}

func decodeSolanaSignature(value string) ([]byte, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, fmt.Errorf("empty signature")
	}

	if decoded, err := decodeBase58(trimmed); err == nil && len(decoded) == ed25519.SignatureSize {
		return decoded, nil
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if decoded, err := encoding.DecodeString(trimmed); err == nil && len(decoded) == ed25519.SignatureSize {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("unsupported Solana signature encoding")
}

func verifySolanaOrderSignature(order CreateOrderRequest) error {
	if order.Network != "solana" {
		return nil
	}
	log.Printf("[solana-verify] incoming order maker=%s pair=%s side=%s amount=%s amountIn=%s amountOutMin=%s expiration=%d signature=%q",
		order.Maker, order.PairID, order.Side, order.Amount, order.AmountIn, order.AmountOutMin, order.Expiration, order.Signature)
	publicKey, err := decodeBase58(order.Maker)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		log.Printf("[solana-verify] invalid maker public key maker=%q err=%v", order.Maker, err)
		return fmt.Errorf("invalid Solana maker public key")
	}

	signature, err := decodeSolanaSignature(order.Signature)
	if err != nil {
		log.Printf("[solana-verify] invalid signature encoding maker=%q signature=%q err=%v", order.Maker, order.Signature, err)
		return fmt.Errorf("invalid Solana signature encoding")
	}

	body := canonicalSolanaOrderBody(order)
	messageCandidates := [][]byte{
		append([]byte("Aster DEX Order:\n"), body...),
		body,
	}
	for _, message := range messageCandidates {
		if ed25519.Verify(ed25519.PublicKey(publicKey), message, signature) {
			log.Printf("[solana-verify] signature ok maker=%s pair=%s", order.Maker, order.PairID)
			return nil
		}
	}
	log.Printf("[solana-verify] signature mismatch maker=%s pair=%s body=%s", order.Maker, order.PairID, string(body))
	return fmt.Errorf("invalid Solana order signature")
}

type SettlementResult struct {
	Status   string                 `json:"status"`
	TxHash   string                 `json:"txHash"`
	Error    string                 `json:"error"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

const settlementLeaseDuration = 2 * time.Minute

var pairCandleIntervals = map[string]struct{}{
	"1m": {}, "5m": {}, "15m": {}, "1h": {}, "4h": {}, "1d": {},
}

// intervalSeconds maps each supported candle interval to its bucket size in seconds.
var intervalSeconds = map[string]int64{
	"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400,
}

// ── Token price cache (CoinGecko) ─────────────────────────────────────────────

type tokenPriceCache struct {
	mu         sync.RWMutex
	prices     map[string]float64 // token symbol -> USD price
	lastUpdate time.Time
	ttl        time.Duration
}

var globalTokenPriceCache = &tokenPriceCache{
	prices: make(map[string]float64),
	ttl:    5 * time.Minute,
}

type tokenDecimalsCache struct {
	mu       sync.RWMutex
	decimals map[string]int
}

var globalTokenDecimalsCache = &tokenDecimalsCache{decimals: make(map[string]int)}

func (c *tokenDecimalsCache) get(token string) (int, bool) {
	key := strings.ToLower(strings.TrimSpace(token))
	if key == "" {
		return 0, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	decimals, ok := c.decimals[key]
	return decimals, ok
}

func (c *tokenDecimalsCache) set(token string, decimals int) {
	key := strings.ToLower(strings.TrimSpace(token))
	if key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.decimals[key] = decimals
}

func tokenDecimalRpcUrls() []string {
	urls := []string{
		os.Getenv("BASE_RPC_URL"),
		os.Getenv("BSC_RPC_URL"),
		os.Getenv("ROBINHOOD_RPC_URL"),
		os.Getenv("SOLANA_RPC_URL"),
	}
	seen := make(map[string]struct{}, len(urls))
	out := make([]string, 0, len(urls))
	for _, url := range urls {
		trimmed := strings.TrimSpace(url)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func fetchTokenDecimalsFromRPC(tokenAddress string) (int, error) {
	trimmed := strings.TrimSpace(tokenAddress)
	if trimmed == "" || !strings.HasPrefix(strings.ToLower(trimmed), "0x") {
		return 0, fmt.Errorf("not an EVM token address")
	}
	for _, rpcURL := range tokenDecimalRpcUrls() {
		body, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"method":  "eth_call",
			"params":  []any{map[string]string{"to": trimmed, "data": "0x313ce567"}, "latest"},
			"id":      1,
		})
		if err != nil {
			continue
		}
		client := &http.Client{Timeout: 6 * time.Second}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, rpcURL, bytes.NewReader(body))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		var payload struct {
			Result string `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			continue
		}
		if payload.Error != nil {
			continue
		}
		if payload.Result == "" || payload.Result == "0x" {
			continue
		}
		hexValue := strings.TrimPrefix(payload.Result, "0x")
		if len(hexValue) > 64 {
			hexValue = hexValue[len(hexValue)-64:]
		}
		if len(hexValue) < 64 {
			hexValue = strings.Repeat("0", 64-len(hexValue)) + hexValue
		}
		decoded, err := hex.DecodeString(hexValue)
		if err != nil {
			continue
		}
		value := new(big.Int).SetBytes(decoded)
		if value.Sign() < 0 || value.BitLen() > 32 {
			continue
		}
		decimals := int(value.Int64())
		if decimals >= 0 && decimals <= 255 {
			globalTokenDecimalsCache.set(trimmed, decimals)
			return decimals, nil
		}
	}
	return 0, fmt.Errorf("token decimals unavailable on RPC")
}

func resolveQuoteTokenDecimalsFromMetadata(token string) int {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return 18
	}
	if decimals, ok := globalTokenDecimalsCache.get(trimmed); ok {
		return decimals
	}
	lower := strings.ToLower(trimmed)
	addressDecimals := map[string]int{
		"0x4200000000000000000000000000000000000006": 18,
		"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 18,
		"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": 6,
		"0x50c5725949a6f0c72e6c4a641f24049a917db0cb": 18,
		"0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": 18,
		"0x0000000000000000000000000000000000000000": 18,
	}
	if value, ok := addressDecimals[lower]; ok {
		globalTokenDecimalsCache.set(trimmed, value)
		return value
	}
	if strings.HasPrefix(lower, "0x") {
		if decimals, err := fetchTokenDecimalsFromRPC(trimmed); err == nil {
			return decimals
		}
	}
	upper := strings.ToUpper(trimmed)
	switch upper {
	case "SOL":
		globalTokenDecimalsCache.set(trimmed, 9)
		return 9
	case "USDC", "USDT", "USDBC", "USDCE", "USDP", "DAI":
		globalTokenDecimalsCache.set(trimmed, 6)
		return 6
	case "WETH", "WBNB", "ETH", "WBTC", "BTC", "MATIC", "ARB", "OP", "AVAX", "NEAR", "LINK", "UNI", "PEPE":
		globalTokenDecimalsCache.set(trimmed, 18)
		return 18
	default:
		if strings.Contains(upper, "USDC") || strings.Contains(upper, "USDT") || strings.Contains(upper, "DAI") {
			globalTokenDecimalsCache.set(trimmed, 6)
			return 6
		}
		if strings.Contains(upper, "SOL") || strings.Contains(upper, "WETH") || strings.Contains(upper, "WBNB") || strings.Contains(upper, "ETH") {
			if strings.Contains(upper, "SOL") {
				globalTokenDecimalsCache.set(trimmed, 9)
				return 9
			}
			globalTokenDecimalsCache.set(trimmed, 18)
			return 18
		}
		globalTokenDecimalsCache.set(trimmed, 18)
		return 18
	}
}

// Token address to CoinGecko ID mapping
var tokenToCoinGeckoID = map[string]string{
	"So11111111111111111111111111111111111111112":  "solana",   // SOL
	"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2":   "weth",     // WETH (Ethereum mainnet)
	"0x4200000000000000000000000000000000000006":   "weth",     // WETH (Base mainnet)
	"0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c":   "wbnb",     // WBNB
	"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "usd-coin", // USDC (Solana)
	"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "tether",   // USDT (Solana)
	"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913":   "usd-coin", // USDC (Base)
	"0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb":   "dai",      // DAI (Base)
	"0x0000000000000000000000000000000000000000":   "wrapped-bitcoin",
}

// Stablecoins always return $1
var stablecoins = map[string]bool{
	"USDC": true, "USDT": true, "USDG": true, "DAI": true, "BUSD": true,
}

func (c *tokenPriceCache) getPrice(tokenAddressOrSymbol string) (float64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check if it's a stablecoin symbol
	upperSymbol := strings.ToUpper(tokenAddressOrSymbol)
	if stablecoins[upperSymbol] {
		return 1.0, true
	}

	// Check cache
	if price, ok := c.prices[tokenAddressOrSymbol]; ok && time.Since(c.lastUpdate) < c.ttl {
		return price, true
	}
	return 0, false
}

func (c *tokenPriceCache) updatePrices() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Rate limit check
	if time.Since(c.lastUpdate) < c.ttl {
		return nil
	}

	// Fetch from CoinGecko Simple Price API (free tier)
	ids := []string{"solana", "weth", "wbnb"}
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd", strings.Join(ids, ","))

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("coingecko returned %d", resp.StatusCode)
	}

	var result map[string]map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	// Update cache with token addresses and symbols
	for geckoID, prices := range result {
		if usdPrice, ok := prices["usd"]; ok {
			// Map by CoinGecko ID
			c.prices[geckoID] = usdPrice

			// Also map by token address
			for addr, id := range tokenToCoinGeckoID {
				if id == geckoID {
					c.prices[addr] = usdPrice
					// Also map by symbol
					switch geckoID {
					case "solana":
						c.prices["SOL"] = usdPrice
					case "weth":
						c.prices["WETH"] = usdPrice
					case "wbnb":
						c.prices["WBNB"] = usdPrice
					}
				}
			}
		}
	}

	c.lastUpdate = time.Now()
	log.Printf("[token-price-cache] Updated prices from CoinGecko: SOL=$%.2f WETH=$%.2f WBNB=$%.2f",
		c.prices["SOL"], c.prices["WETH"], c.prices["WBNB"])
	return nil
}

// Get token price by address or symbol, fetching from CoinGecko if needed
func getTokenPrice(tokenAddressOrSymbol string) float64 {
	// Try cache first
	if price, ok := globalTokenPriceCache.getPrice(tokenAddressOrSymbol); ok {
		return price
	}

	// Update cache (respects TTL internally)
	_ = globalTokenPriceCache.updatePrices()

	// Try again
	if price, ok := globalTokenPriceCache.getPrice(tokenAddressOrSymbol); ok {
		return price
	}

	return 0
}

func resolveQuoteFillDecimals(quoteAddress, quoteSymbol string) int {
	if strings.TrimSpace(quoteAddress) != "" {
		decoded := resolveQuoteTokenDecimalsFromMetadata(quoteAddress)
		if decoded > 0 {
			return decoded
		}
	}
	if strings.TrimSpace(quoteSymbol) != "" {
		return resolveQuoteTokenDecimalsFromMetadata(quoteSymbol)
	}
	return 18
}

func normalizeFillAmountQuote(amountQuote, quoteAddress, quoteSymbol string) float64 {
	amountQuote = strings.TrimSpace(amountQuote)
	if amountQuote == "" {
		return 0
	}
	amount, ok := new(big.Float).SetString(amountQuote)
	if !ok {
		return 0
	}
	decimals := resolveQuoteFillDecimals(quoteAddress, quoteSymbol)
	divisor := new(big.Float).SetFloat64(math.Pow10(decimals))
	amountNormalized, _ := new(big.Float).Quo(amount, divisor).Float64()
	return amountNormalized
}

func sumConfirmedFillVolume24h(ctx context.Context, db *sql.DB, pairID string) (float64, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT COALESCE(NULLIF(amount_quote, ''), '0'), COALESCE(NULLIF(quote_token_address, ''), ''), COALESCE(NULLIF(quote_token_symbol, ''), '')
		FROM dex_fills
		WHERE pair_id=$1 AND settlement_status='confirmed' AND created_at >= NOW() - INTERVAL '24 hours'`, pairID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	total := 0.0
	for rows.Next() {
		var amountQuote, quoteAddress, quoteSymbol string
		if err := rows.Scan(&amountQuote, &quoteAddress, &quoteSymbol); err != nil {
			return 0, err
		}
		total += normalizeFillAmountQuote(amountQuote, quoteAddress, quoteSymbol)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return total, nil
}

func quoteVolumeLookupCandidates(quoteAddress, quoteSymbol string) []string {
	candidates := make([]string, 0, 2)
	quoteAddress = strings.TrimSpace(quoteAddress)
	quoteSymbol = strings.TrimSpace(quoteSymbol)
	if quoteAddress != "" {
		candidates = append(candidates, quoteAddress)
	}
	if quoteSymbol != "" {
		candidates = append(candidates, quoteSymbol)
	}
	return candidates
}

type pairCache struct {
	mu        sync.RWMutex
	entries   map[string][]Pair
	updatedAt map[string]time.Time
	ttl       time.Duration
}

type pairHub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]*sync.Mutex
}

func newPairCache(ttl time.Duration) *pairCache {
	return &pairCache{
		entries:   make(map[string][]Pair),
		updatedAt: make(map[string]time.Time),
		ttl:       ttl,
	}
}

func newPairHub() *pairHub {
	return &pairHub{clients: make(map[*websocket.Conn]*sync.Mutex)}
}

func (h *pairHub) add(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.clients[conn]; !exists {
		h.clients[conn] = &sync.Mutex{}
	}
}

func (h *pairHub) remove(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, conn)
}

func (h *pairHub) broadcast(payload any) {
	h.mu.RLock()
	clients := make([]struct {
		conn *websocket.Conn
		mu   *sync.Mutex
	}, 0, len(h.clients))
	for client, writeMu := range h.clients {
		clients = append(clients, struct {
			conn *websocket.Conn
			mu   *sync.Mutex
		}{conn: client, mu: writeMu})
	}
	h.mu.RUnlock()

	log.Printf("[hub] Broadcasting to %d clients: %+v", len(clients), payload)
	for _, client := range clients {
		client.mu.Lock()
		if err := client.conn.WriteJSON(payload); err != nil {
			client.mu.Unlock()
			log.Printf("[hub] Failed to send to client: %v", err)
			_ = client.conn.Close()
			h.remove(client.conn)
			continue
		}
		client.mu.Unlock()
		log.Printf("[hub] Successfully sent to client")
	}
}

func normalizeNetwork(network string) string {
	network = strings.ToLower(strings.TrimSpace(network))
	if network == "" || network == "all" {
		return "all"
	}
	return network
}

func dbNetworkFilter(network string) string {
	network = strings.ToLower(strings.TrimSpace(network))
	if network == "" || network == "all" {
		return ""
	}
	return network
}

func (c *pairCache) get(ctx context.Context, db *sql.DB, network string, limit int) ([]Pair, error) {
	key := normalizeNetwork(network)
	c.mu.RLock()
	cached, found := c.entries[key]
	lastUpdated, hasUpdated := c.updatedAt[key]
	c.mu.RUnlock()
	if found && hasUpdated && time.Since(lastUpdated) < c.ttl {
		return clonePairs(cached), nil
	}
	return c.refresh(ctx, db, key, limit)
}

func (c *pairCache) refresh(ctx context.Context, db *sql.DB, network string, limit int) ([]Pair, error) {
	pairs, err := queryPairs(ctx, db, network, limit)
	if err != nil {
		return nil, err
	}
	pairs = rankActivePairs(pairs, limit)
	c.mu.Lock()
	c.entries[network] = clonePairs(pairs)
	c.updatedAt[network] = time.Now()
	c.mu.Unlock()
	return clonePairs(pairs), nil
}

func rankActivePairs(pairs []Pair, limit int) []Pair {
	if len(pairs) == 0 {
		return nil
	}

	scored := make([]Pair, 0, len(pairs))
	for _, pair := range pairs {
		score := scoreActivePair(pair)
		if score <= 0 && pair.Price == nil {
			continue
		}
		pair.Volume24h = ensureFloat(pair.Volume24h, 0)
		pair.Liquidity = ensureFloat(pair.Liquidity, 0)
		scored = append(scored, pair)
	}
	if len(scored) == 0 {
		scored = pairs
	}

	sort.Slice(scored, func(i, j int) bool {
		leftScore := scoreActivePair(scored[i])
		rightScore := scoreActivePair(scored[j])
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		leftVol := 0.0
		if scored[i].Volume24h != nil {
			leftVol = *scored[i].Volume24h
		}
		rightVol := 0.0
		if scored[j].Volume24h != nil {
			rightVol = *scored[j].Volume24h
		}
		if leftVol != rightVol {
			return leftVol > rightVol
		}
		leftLiq := 0.0
		if scored[i].Liquidity != nil {
			leftLiq = *scored[i].Liquidity
		}
		rightLiq := 0.0
		if scored[j].Liquidity != nil {
			rightLiq = *scored[j].Liquidity
		}
		if leftLiq != rightLiq {
			return leftLiq > rightLiq
		}
		if scored[i].UpdatedAt != nil && scored[j].UpdatedAt != nil {
			return *scored[i].UpdatedAt > *scored[j].UpdatedAt
		}
		return scored[i].Symbol < scored[j].Symbol
	})

	if limit > 0 && len(scored) > limit {
		scored = scored[:limit]
	}
	return scored
}

func scoreActivePair(pair Pair) float64 {
	vol := 0.0
	if pair.Volume24h != nil {
		vol = *pair.Volume24h
	}
	liq := 0.0
	if pair.Liquidity != nil {
		liq = *pair.Liquidity
	}
	price := 0.0
	if pair.Price != nil {
		price = *pair.Price
	}
	change := 0.0
	if pair.PriceChange != nil {
		change = *pair.PriceChange
	}
	tx1h := 0.0
	if pair.TransactionCount1h != nil {
		tx1h = *pair.TransactionCount1h
	}
	tx24h := 0.0
	if pair.TransactionCount24h != nil {
		tx24h = *pair.TransactionCount24h
	}
	if vol == 0 && liq == 0 && price == 0 && tx1h == 0 && tx24h == 0 {
		return 0
	}
	return (tx1h * 1000.0) + (tx24h * 50.0) + (vol * 1.0) + (liq * 0.75) + (mathAbs(change) * 10.0) + (price * 0.000001)
}

func ensureFloat(value *float64, fallback float64) *float64 {
	if value != nil {
		return value
	}
	clone := fallback
	return &clone
}

func mathAbs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func clonePairs(pairs []Pair) []Pair {
	if len(pairs) == 0 {
		return nil
	}
	out := make([]Pair, len(pairs))
	copy(out, pairs)
	return out
}

var pairQuery = `
SELECT address, pool_type, network, base_symbol, quote_symbol, base_mint, quote_mint,
	  base_logo_url, quote_logo_url, price, inverse_price,
	  (SELECT token_price_usd FROM latest_prices lp2 WHERE lp2.pool_address = pairs.address),
	  price_change_percent, high_24h, low_24h,
	  CAST(NULL AS DOUBLE PRECISION) as liquidity, CAST(NULL AS DOUBLE PRECISION) as volume_24h, updated_at
FROM (
  SELECT p.address, p.pool_type, p.network, p.base_symbol, p.quote_symbol, p.base_mint, p.quote_mint,
	  p.base_logo_url, p.quote_logo_url, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at
  FROM pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.network, p.token_mint_0_symbol, p.token_mint_1_symbol, p.token_mint_0, p.token_mint_1,
	  p.token_mint_0_logo_url, p.token_mint_1_logo_url, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM raydium_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.network, p.token_a_symbol, p.token_b_symbol, p.token_a_mint, p.token_b_mint,
	  p.token_a_logo_url, p.token_b_logo_url, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM meteora_damm_v2_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.network, p.token_x_symbol, p.token_y_symbol, p.token_x_mint, p.token_y_mint,
	  p.token_x_logo_url, p.token_y_logo_url, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM meteora_dlmm_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.network, p.token_mint_a_symbol, p.token_mint_b_symbol, p.token_mint_a, p.token_mint_b,
	  p.token_mint_a_logo_url, p.token_mint_b_logo_url, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM orca_whirlpools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.token0_symbol, p.token1_symbol, p.token0, p.token1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM bsc_pancakeswap_v2_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.token0_symbol, p.token1_symbol, p.token0, p.token1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM bsc_pancakeswap_v3_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.token0_symbol, p.token1_symbol, p.token0, p.token1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM bsc_uniswap_v3_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.currency0_symbol, p.currency1_symbol, p.currency0, p.currency1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM bsc_uniswap_v4_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.currency0_symbol, p.currency1_symbol, p.currency0, p.currency1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM base_uniswap_v4_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.token0_symbol, p.token1_symbol, p.token0, p.token1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM base_uniswap_v3_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.token0_symbol, p.token1_symbol, p.token0, p.token1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM aerodrome_slipstream_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.currency0_symbol, p.currency1_symbol, p.currency0, p.currency1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM bsc_pancakeswap_infinity_cl_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.token0_symbol, p.token1_symbol, p.token0, p.token1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM robinhood_uniswap_v2_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.token0_symbol, p.token1_symbol, p.token0, p.token1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM robinhood_uniswap_v3_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
  UNION ALL
  SELECT p.address, p.pool_type, p.chain, p.currency0_symbol, p.currency1_symbol, p.currency0, p.currency1,
	  NULL::text, NULL::text, lp.price, lp.inverse_price, lp.price_change_percent, lp.high_24h, lp.low_24h, lp.updated_at FROM robinhood_uniswap_v4_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
) pairs
WHERE ($1 = '' OR network = $1)
ORDER BY (price IS NULL), updated_at DESC NULLS LAST, network, COALESCE(base_symbol, base_mint), COALESCE(quote_symbol, quote_mint)
LIMIT $2`

func main() {
	loadEnvFile(".env")
	loadEnvFile("../.env")
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgresql://postgres:postgres@127.0.0.1:54322/postgres?sslmode=disable"
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := ensureOrdersTable(db); err != nil {
		log.Fatal(err)
	}

	if err := ensurePairEventTriggers(db); err != nil {
		log.Printf("pair notification trigger setup failed: %v", err)
	}

	cache := newPairCache(30 * time.Second)
	for _, network := range []string{"", "all", "bsc", "base", "solana", "robinhood"} {
		if _, err := cache.refresh(context.Background(), db, network, 400); err != nil {
			log.Printf("initial pair cache warm failed for %q: %v", network, err)
		}
	}
	hub := newPairHub()
	go listenForPairEvents(db, cache, hub)
	go reconcileOpenOrders(db, hub)
	go checkConditionalOrders(db, hub)
	go sweepExpiredOrders(db, hub)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler(db))
	mux.HandleFunc("GET /api/pairs", pairsHandler(db, nil, cache))
	mux.HandleFunc("GET /api/pairs/search", searchPairsHandler(db, cache))
	mux.HandleFunc("GET /api/pairs/ws", wsPairHandler(hub))
	mux.HandleFunc("GET /api/pairs/{id}/candles", pairCandlesHandler(db))
	mux.HandleFunc("GET /api/pairs/{id}/decimals", pairDecimalsHandler(db))
	mux.HandleFunc("POST /api/orders", createOrderHandler(db, hub))
	mux.HandleFunc("GET /api/orders", listOrdersHandler(db))
	mux.HandleFunc("GET /api/orders/fills", listFillsHandler(db))
	mux.HandleFunc("GET /api/settlement/pending", pendingSettlementHandler(db))
	mux.HandleFunc("POST /api/settlement/fills/{id}/claim", claimSettlementHandler(db))
	mux.HandleFunc("POST /api/settlement/fills/{id}/result", settlementResultHandler(db, hub))
	mux.HandleFunc("GET /api/settlement/health", settlementHealthHandler(db))
	mux.HandleFunc("DELETE /api/orders/{id}", cancelOrderHandler(db, hub))
	mux.HandleFunc("GET /api/pairs/active", pairsHandler(db, nil, cache))
	mux.HandleFunc("GET /api/pairs/all", pairsHandler(db, stringPtr(""), cache))
	mux.HandleFunc("GET /api/pairs/all/active", pairsHandler(db, stringPtr(""), cache))
	mux.HandleFunc("GET /api/pairs/bsc", pairsHandler(db, stringPtr("bsc"), cache))
	mux.HandleFunc("GET /api/pairs/bsc/active", pairsHandler(db, stringPtr("bsc"), cache))
	mux.HandleFunc("GET /api/pairs/base", pairsHandler(db, stringPtr("base"), cache))
	mux.HandleFunc("GET /api/pairs/base/active", pairsHandler(db, stringPtr("base"), cache))
	mux.HandleFunc("GET /api/pairs/solana", pairsHandler(db, stringPtr("solana"), cache))
	mux.HandleFunc("GET /api/pairs/solana/active", pairsHandler(db, stringPtr("solana"), cache))
	mux.HandleFunc("GET /api/pairs/robinhood", pairsHandler(db, stringPtr("robinhood"), cache))
	mux.HandleFunc("GET /api/pairs/robinhood/active", pairsHandler(db, stringPtr("robinhood"), cache))
	mux.HandleFunc("POST /api/solana/rpc", solanaRPCProxyHandler())

	port := os.Getenv("PORT")
	if port == "" {
		port = "8787"
	}
	server := &http.Server{Addr: ":" + port, Handler: withCORS(mux), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("DEX backend listening on http://127.0.0.1:%s", port)
	log.Fatal(server.ListenAndServe())
}

// checkConditionalOrders polls every 3 s for stop_loss / take_profit orders whose
// trigger price has been crossed by the last confirmed fill price on that pair,
// then converts them to immediately-matched limit orders.
//
// Trigger semantics (industry standard):
//   - stop_loss  SELL: fires when last fill price <= trigger_price (price fell to stop)
//   - stop_loss  BUY:  fires when last fill price >= trigger_price (used for short covers)
//   - take_profit SELL: fires when last fill price >= trigger_price (price rose to target)
//   - take_profit BUY:  fires when last fill price <= trigger_price (buying a dip to a target)
func resolveTriggeredConditionalOrder(executionType, currentMarketPrice, triggerPrice string) (string, string) {
	executionType = strings.TrimSpace(strings.ToLower(executionType))
	if executionType == "market" {
		// Market conditional orders are not given a fixed execution price. They must
		// immediately match against the best resting orders in the book that satisfy
		// the trigger condition. Keeping a synthetic market price here causes the
		// triggered order to look like a limit fill at the trigger/current value,
		// which is not how industry-standard market TP/SL order matching works.
		return "market", ""
	}
	return "limit", strings.TrimSpace(triggerPrice)
}

func sanitizeConditionalOrderRequest(req *CreateOrderRequest) {
	if req == nil || req.OrderType == "" || !(req.OrderType == "stop_loss" || req.OrderType == "take_profit") {
		return
	}
	if strings.TrimSpace(strings.ToLower(req.ExecutionType)) == "market" {
		req.Price = ""
	}
}

func sanitizeMarketOrderRequest(req *CreateOrderRequest) {
	if req == nil || req.OrderType == "" || strings.TrimSpace(strings.ToLower(req.OrderType)) != "market" {
		return
	}
	req.Price = ""
	req.AmountOutMin = "0"
}

func resolveMarketReferencePrice(candidates ...string) (*big.Rat, bool) {
	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		if trimmed == "" {
			continue
		}
		price, err := parsePrice(trimmed)
		if err == nil {
			return price, true
		}
	}
	return nil, false
}

func livePriceForPair(db *sql.DB, pairID string) (string, bool) {
	var price sql.NullString
	if err := db.QueryRow(`SELECT price FROM latest_prices WHERE pool_address=$1 LIMIT 1`, pairID).Scan(&price); err == nil && price.Valid && strings.TrimSpace(price.String) != "" {
		return strings.TrimSpace(price.String), true
	}
	if err := db.QueryRow(`SELECT price FROM dex_fills WHERE pair_id=$1 AND settlement_status='confirmed' ORDER BY created_at DESC LIMIT 1`, pairID).Scan(&price); err == nil && price.Valid && strings.TrimSpace(price.String) != "" {
		return strings.TrimSpace(price.String), true
	}
	return "", false
}

func marketOrderReferencePrice(db interface {
	QueryRow(string, ...interface{}) *sql.Row
}, pairID string) (*big.Rat, bool) {
	if db == nil || strings.TrimSpace(pairID) == "" {
		return nil, false
	}
	var price sql.NullString
	if err := db.QueryRow(`SELECT price FROM latest_prices WHERE pool_address=$1 LIMIT 1`, pairID).Scan(&price); err == nil && price.Valid && strings.TrimSpace(price.String) != "" {
		if ref, ok := resolveMarketReferencePrice(price.String); ok {
			return ref, true
		}
	}
	if err := db.QueryRow(`SELECT price FROM dex_fills WHERE pair_id=$1 AND settlement_status='confirmed' ORDER BY created_at DESC LIMIT 1`, pairID).Scan(&price); err == nil && price.Valid && strings.TrimSpace(price.String) != "" {
		if ref, ok := resolveMarketReferencePrice(price.String); ok {
			return ref, true
		}
	}
	return nil, false
}

func checkConditionalOrders(db *sql.DB, hub *pairHub) {
	for {
		time.Sleep(3 * time.Second)
		// Fetch the live market price from the pool index, not the last fill.
		// The last fill can be stale and is not the real current pair price.
		priceRows, err := db.Query(`
			SELECT pool_address, price::double precision
			FROM latest_prices
			WHERE price IS NOT NULL`)
		if err != nil {
			log.Printf("[TP/SL] price query failed: %v", err)
			continue
		}
		type pairPrice struct{ pairID, price string }
		prices := make([]pairPrice, 0)
		for priceRows.Next() {
			var pp pairPrice
			if err := priceRows.Scan(&pp.pairID, &pp.price); err != nil {
				break
			}
			prices = append(prices, pp)
		}
		_ = priceRows.Close()

		for _, pp := range prices {
			currentPrice, err := parsePrice(pp.price)
			if err != nil {
				continue
			}

			// Query all pending conditional orders for this pair.
			condRows, err := db.Query(`
				SELECT id, order_type, execution_type, side, trigger_price
				FROM dex_orders
				WHERE pair_id = $1
				  AND order_type IN ('stop_loss', 'take_profit')
				  AND status = 'pending'
				  AND trigger_price <> ''
				  AND expiration > EXTRACT(EPOCH FROM NOW())`, pp.pairID)
			if err != nil {
				log.Printf("[TP/SL] cond query failed for %s: %v", pp.pairID, err)
				continue
			}

			type condOrder struct {
				id            int64
				orderType     string
				executionType string
				side          string
				triggerPrice  string
			}
			orders := make([]condOrder, 0)
			for condRows.Next() {
				var o condOrder
				if err := condRows.Scan(&o.id, &o.orderType, &o.executionType, &o.side, &o.triggerPrice); err == nil {
					orders = append(orders, o)
				}
			}
			_ = condRows.Close()

			for _, o := range orders {
				tp, err := parsePrice(o.triggerPrice)
				if err != nil {
					continue
				}

				triggered := false
				switch {
				case o.orderType == "stop_loss" && o.side == "sell":
					// Sell stop-loss: triggers when price drops to or below trigger.
					triggered = currentPrice.Cmp(tp) <= 0
				case o.orderType == "stop_loss" && o.side == "buy":
					// Buy stop-loss (short cover): triggers when price rises to or above trigger.
					triggered = currentPrice.Cmp(tp) >= 0
				case o.orderType == "take_profit" && o.side == "sell":
					// Sell take-profit: triggers when price rises to or above trigger.
					triggered = currentPrice.Cmp(tp) >= 0
				case o.orderType == "take_profit" && o.side == "buy":
					// Buy take-profit (dip buy): triggers when price drops to or below trigger.
					triggered = currentPrice.Cmp(tp) <= 0
				}

				if !triggered {
					continue
				}

				// Promote to the correct execution model. Market orders must execute at the
				// current market price when triggered; limit orders use the trigger price.
				promotedOrderType, promotedPrice := resolveTriggeredConditionalOrder(o.executionType, pp.price, o.triggerPrice)
				res, err := db.Exec(`
					UPDATE dex_orders
					SET order_type = $1,
						status = 'open',
						price = $2,
						triggered_at = NOW(),
						updated_at = NOW()
					WHERE id = $3 AND status = 'pending'
						AND order_type IN ('stop_loss', 'take_profit')`, promotedOrderType, promotedPrice, o.id)
				if err != nil {
					log.Printf("[TP/SL] promote id=%d failed: %v", o.id, err)
					continue
				}
				if n, _ := res.RowsAffected(); n == 0 {
					continue // already triggered by a concurrent loop
				}
				log.Printf("[TP/SL] triggered %s %s id=%d at market price %s (trigger %s)",
					o.orderType, o.side, o.id, pp.price, o.triggerPrice)

				// Now match it as a normal limit order.
				fills, matchErr := matchOrder(context.Background(), db, o.id)
				if matchErr != nil {
					log.Printf("[TP/SL] match id=%d failed: %v", o.id, matchErr)
					continue
				}
				for _, fill := range fills {
					hub.broadcast(map[string]any{"type": "order_fill", "fill": fill})
					broadcastPairUpdate(context.Background(), db, hub, fill.PairID)
				}
				// Broadcast the order status update so the UI reflects the triggered state.
				var triggeredOrder StoredOrder
				if err := scanOrder(db.QueryRowContext(context.Background(),
					`SELECT `+orderFields+` FROM dex_orders WHERE id=$1`, o.id),
					&triggeredOrder); err == nil {
					hub.broadcast(map[string]any{"type": "order_update", "order": triggeredOrder})
				}
			}
		}
	}
}

// sweepExpiredOrders runs every 30 s and marks any open/pending/partial orders
// whose expiration timestamp has passed as 'expired'. It also broadcasts an
// order_update WS event for each newly expired order so the UI updates in real
// time. Running this as a dedicated goroutine means expiry is authoritative and
// consistent regardless of whether any client calls listOrders.
func sweepExpiredOrders(db *sql.DB, hub *pairHub) {
	for {
		time.Sleep(30 * time.Second)
		rows, err := db.Query(
			`UPDATE dex_orders
			 SET status = 'expired', updated_at = NOW()
			 WHERE status IN ('pending', 'open', 'partial')
			   AND expiration <= EXTRACT(EPOCH FROM NOW())
			 RETURNING id, order_hash, maker, pair_id, network, side, order_type,
			           price, trigger_price, amount, filled_amount, amount_in, amount_out_min,
			           token_in, token_out, receiver, signature, expiration, nonce, salt,
			           status, created_at::text, updated_at::text, ladder_levels, ladder_price_start,
			           ladder_price_end, ladder_level, ladder_parent_hash, ladder_total_amount, is_ladder, post_only`)
		if err != nil {
			log.Printf("[expiry] sweep failed: %v", err)
			continue
		}
		pairsToRefresh := make(map[string]struct{})
		for rows.Next() {
			var o StoredOrder
			if err := rows.Scan(
				&o.ID, &o.OrderHash, &o.Maker, &o.PairID, &o.Network, &o.Side, &o.OrderType,
				&o.Price, &o.TriggerPrice, &o.Amount, &o.FilledAmount, &o.AmountIn, &o.AmountOutMin,
				&o.TokenIn, &o.TokenOut, &o.Receiver, &o.Signature, &o.Expiration, &o.Nonce, &o.Salt,
				&o.Status, &o.CreatedAt, &o.UpdatedAt, &o.LadderLevels, &o.LadderPriceStart, &o.LadderPriceEnd,
				&o.LadderLevel, &o.LadderParentHash, &o.LadderTotalAmount, &o.IsLadder, &o.PostOnly,
			); err != nil {
				log.Printf("[expiry] scan failed: %v", err)
				continue
			}
			hub.broadcast(map[string]any{"type": "order_update", "order": o})
			log.Printf("[expiry] Broadcasting order_update for expired order id=%d maker=%s status=%s", o.ID, o.Maker, o.Status)
			pairsToRefresh[o.PairID] = struct{}{}
		}
		_ = rows.Close()
		for pairID := range pairsToRefresh {
			broadcastPairUpdate(context.Background(), db, hub, pairID)
		}
	}
}

func reconcileOpenOrders(db *sql.DB, hub *pairHub) {
	for {
		rows, err := db.Query(`SELECT id FROM dex_orders WHERE LOWER(network) IN ('bsc','base','robinhood','solana') AND status IN ('pending','open','partial') AND expiration > EXTRACT(EPOCH FROM NOW()) ORDER BY created_at ASC LIMIT 100`)
		if err != nil {
			log.Printf("order reconciliation query failed: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		ids := make([]int64, 0, 100)
		for rows.Next() {
			var id int64
			if scanErr := rows.Scan(&id); scanErr != nil {
				log.Printf("order reconciliation scan failed: %v", scanErr)
				break
			}
			ids = append(ids, id)
		}
		_ = rows.Close()
		for _, id := range ids {
			fills, matchErr := matchOrder(context.Background(), db, id)
			if matchErr != nil {
				log.Printf("order reconciliation id=%d failed: %v", id, matchErr)
				continue
			}
			for _, fill := range fills {
				log.Printf("order reconciliation matched fill=%d maker=%d taker=%d amount=%s price=%s", fill.ID, fill.MakerOrderID, fill.TakerOrderID, fill.Amount, fill.Price)
				hub.broadcast(map[string]any{"type": "order_fill", "fill": fill})
				broadcastPairUpdate(context.Background(), db, hub, fill.PairID)
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func broadcastPairUpdate(ctx context.Context, db *sql.DB, hub *pairHub, pairID string) {
	// Run async with its own timeout — never block the calling request handler.
	// Determine the network directly from the pool tables to avoid fetching all 400 pairs.
	go func() {
		bctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Resolve network from the pairID — fastest path is a direct pool table lookup.
		network := "bsc" // default fallback
		var n string
		if err := db.QueryRowContext(bctx, `
			SELECT 'bsc'        FROM bsc_pancakeswap_v2_pools WHERE address=$1
			UNION ALL SELECT 'bsc'        FROM bsc_pancakeswap_v3_pools WHERE address=$1
			UNION ALL SELECT 'bsc'        FROM bsc_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT 'bsc'        FROM bsc_uniswap_v4_pools WHERE address=$1
			UNION ALL SELECT 'base'       FROM base_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT 'base'       FROM base_uniswap_v4_pools WHERE address=$1
			UNION ALL SELECT 'base'       FROM aerodrome_slipstream_pools WHERE address=$1
			UNION ALL SELECT 'robinhood'  FROM robinhood_uniswap_v2_pools WHERE address=$1
			UNION ALL SELECT 'robinhood'  FROM robinhood_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT 'robinhood'  FROM robinhood_uniswap_v4_pools WHERE address=$1
			UNION ALL SELECT 'solana'     FROM raydium_pools WHERE address=$1
			UNION ALL SELECT 'solana'     FROM meteora_damm_v2_pools WHERE address=$1
			UNION ALL SELECT 'solana'     FROM meteora_dlmm_pools WHERE address=$1
			UNION ALL SELECT 'solana'     FROM orca_whirlpools WHERE address=$1
			LIMIT 1`, pairID).Scan(&n); err == nil && n != "" {
			network = n
		}

		pairs, err := queryPairs(bctx, db, network, 400)
		if err != nil {
			log.Printf("pair metric update failed for %s: %v", pairID, err)
			return
		}
		for _, pair := range pairs {
			if pair.ID == pairID {
				hub.broadcast(map[string]any{"type": "pair_update", "network": pair.Network, "pairs": []Pair{pair}})
				return
			}
		}
	}()
}

// broadcastCandleUpdate rebuilds the current candle for every interval from the
// latest DEX fills and pushes a "candle_update" message over the WebSocket.
// The frontend's subscribePairCandles listener picks this up to update the chart
// the moment a fill confirms — no polling latency.
func broadcastCandleUpdate(db *sql.DB, hub *pairHub, pairID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for interval := range intervalSeconds {
		candles, err := buildDexCandles(ctx, db, pairID, interval, 0, 0, 1)
		if err != nil || len(candles) == 0 {
			continue
		}
		// Send only the latest (current) candle — the chart library will upsert it.
		latest := candles[len(candles)-1]
		hub.broadcast(map[string]any{
			"type":             "candle_update",
			"pairId":           pairID,
			"interval":         interval,
			"priceOrientation": "pool",
			"candle":           latest,
		})
	}
}

func ensureOrdersTable(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS dex_orders (
		id BIGSERIAL PRIMARY KEY, order_hash TEXT NOT NULL UNIQUE, maker TEXT NOT NULL,
		pair_id TEXT NOT NULL, network TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
		price TEXT NOT NULL, amount TEXT NOT NULL, amount_in TEXT NOT NULL, amount_out_min TEXT NOT NULL,
		token_in TEXT NOT NULL, token_out TEXT NOT NULL, receiver TEXT NOT NULL, signature TEXT NOT NULL,
		expiration BIGINT NOT NULL, nonce NUMERIC(78,0) NOT NULL, salt NUMERIC(78,0) NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS token_in_account TEXT NOT NULL DEFAULT ''; ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS token_out_account TEXT NOT NULL DEFAULT '';`)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	_, err = db.Exec(`ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS filled_amount TEXT NOT NULL DEFAULT '0'; ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(); ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS ladder_levels INTEGER NOT NULL DEFAULT 0; ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS ladder_price_start TEXT NOT NULL DEFAULT ''; ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS ladder_price_end TEXT NOT NULL DEFAULT ''; ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS ladder_level INTEGER NOT NULL DEFAULT 0; ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS ladder_parent_hash TEXT NOT NULL DEFAULT ''; ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS is_ladder BOOLEAN NOT NULL DEFAULT FALSE;`)
	if err != nil {
		return err
	}
	// Add market/TP/SL columns idempotently.
	_, err = db.Exec(`
		ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS trigger_price TEXT NOT NULL DEFAULT '';
		ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS execution_type TEXT NOT NULL DEFAULT 'limit';
		ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS triggered_at TIMESTAMPTZ;
		ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS ladder_total_amount TEXT NOT NULL DEFAULT '';
		ALTER TABLE dex_orders ADD COLUMN IF NOT EXISTS post_only BOOLEAN NOT NULL DEFAULT FALSE;
		CREATE INDEX IF NOT EXISTS idx_dex_orders_trigger ON dex_orders(pair_id, order_type, status)
			WHERE order_type IN ('stop_loss', 'take_profit');
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS dex_fills (id BIGSERIAL PRIMARY KEY, maker_order_id BIGINT NOT NULL REFERENCES dex_orders(id), taker_order_id BIGINT NOT NULL REFERENCES dex_orders(id), pair_id TEXT NOT NULL, price TEXT NOT NULL, amount TEXT NOT NULL, amount_quote TEXT NOT NULL, maker TEXT NOT NULL, taker TEXT NOT NULL, settlement_status TEXT NOT NULL DEFAULT 'pending', tx_hash TEXT NOT NULL DEFAULT '', last_error TEXT NOT NULL DEFAULT '', settlement_attempts INTEGER NOT NULL DEFAULT 0, settled_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()); ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS settlement_status TEXT NOT NULL DEFAULT 'pending'; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS tx_hash TEXT NOT NULL DEFAULT ''; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS tx_hash_buy VARCHAR(100) DEFAULT ''; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS tx_hash_sell VARCHAR(100) DEFAULT ''; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT ''; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS settlement_attempts INTEGER NOT NULL DEFAULT 0; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS last_attempted_at TIMESTAMPTZ; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS settlement_owner TEXT NOT NULL DEFAULT ''; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS settlement_lease_until TIMESTAMPTZ; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS quote_token_symbol VARCHAR(20) DEFAULT ''; ALTER TABLE dex_fills ADD COLUMN IF NOT EXISTS quote_token_address TEXT DEFAULT ''; CREATE INDEX IF NOT EXISTS idx_dex_fills_pair_created ON dex_fills(pair_id, created_at DESC); CREATE INDEX IF NOT EXISTS idx_dex_fills_settlement ON dex_fills(settlement_status, created_at); CREATE INDEX IF NOT EXISTS idx_dex_fills_lease ON dex_fills(settlement_status, settlement_lease_until);`)
	return err
}

type matchOrderRow struct {
	ID           int64
	OrderHash    string
	Maker        string
	Side         string
	OrderType    string
	Price        string
	Amount       string
	FilledAmount string
	AmountIn     string
	AmountOutMin string
	TokenIn      string
	TokenOut     string
}

func parseBigInt(value string) (*big.Int, error) {
	parsed := new(big.Int)
	if _, ok := parsed.SetString(strings.TrimSpace(value), 10); !ok || parsed.Sign() < 0 {
		return nil, fmt.Errorf("invalid integer amount")
	}
	return parsed, nil
}

func parsePrice(value string) (*big.Rat, error) {
	parsed, ok := new(big.Rat).SetString(strings.TrimSpace(value))
	if !ok || parsed.Sign() <= 0 {
		return nil, fmt.Errorf("invalid order price")
	}
	return parsed, nil
}

func proportionalAmount(total, fill, amount *big.Int) *big.Int {
	if total.Sign() == 0 || amount.Sign() == 0 {
		return new(big.Int)
	}
	return new(big.Int).Quo(new(big.Int).Mul(total, fill), amount)
}

func computeFillQuoteFromReferencePrice(amountBase *big.Int, referencePrice *big.Rat) *big.Int {
	if amountBase == nil || referencePrice == nil || amountBase.Sign() <= 0 || referencePrice.Sign() <= 0 {
		return new(big.Int)
	}
	quote := new(big.Rat).Mul(new(big.Rat).SetInt(amountBase), referencePrice)
	return new(big.Int).Quo(quote.Num(), quote.Denom())
}

func computeFillPriceFromQuoteAmount(amountBase, amountQuote *big.Int, invert bool) string {
	if amountBase == nil || amountQuote == nil || amountBase.Sign() <= 0 || amountQuote.Sign() < 0 {
		return "0"
	}
	if amountQuote.Sign() == 0 {
		return "0"
	}
	price := new(big.Rat).SetFrac(amountQuote, amountBase)
	if invert {
		price = new(big.Rat).Inv(price)
	}
	text := price.FloatString(18)
	text = strings.TrimRight(text, "0")
	text = strings.TrimRight(text, ".")
	if text == "" || text == "-0" || text == "-0." {
		return "0"
	}
	return text
}

func pairShouldFlipDisplay(tx *sql.Tx, pairID string) bool {
	if tx == nil || strings.TrimSpace(pairID) == "" {
		return false
	}
	var baseSymbol sql.NullString
	err := tx.QueryRowContext(context.Background(), `
		SELECT COALESCE(base_symbol,'') FROM pools WHERE address=$1
		UNION ALL SELECT COALESCE(token0_symbol,'') FROM bsc_pancakeswap_v2_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token0_symbol,'') FROM bsc_pancakeswap_v3_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token0_symbol,'') FROM bsc_uniswap_v3_pools WHERE address=$1
		UNION ALL SELECT COALESCE(currency0_symbol,'') FROM bsc_uniswap_v4_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token0_symbol,'') FROM base_uniswap_v3_pools WHERE address=$1
		UNION ALL SELECT COALESCE(currency0_symbol,'') FROM base_uniswap_v4_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token0_symbol,'') FROM robinhood_uniswap_v2_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token0_symbol,'') FROM robinhood_uniswap_v3_pools WHERE address=$1
		UNION ALL SELECT COALESCE(currency0_symbol,'') FROM robinhood_uniswap_v4_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token_mint_0_symbol,'') FROM raydium_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token_a_symbol,'') FROM meteora_damm_v2_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token_x_symbol,'') FROM meteora_dlmm_pools WHERE address=$1
		UNION ALL SELECT COALESCE(token_mint_a_symbol,'') FROM orca_whirlpools WHERE address=$1
		LIMIT 1`, pairID).Scan(&baseSymbol)
	if err != nil || !baseSymbol.Valid {
		return false
	}
	_, shouldFlip := flipTokens[strings.ToUpper(strings.TrimSpace(baseSymbol.String))]
	return shouldFlip
}

func pricesCross(incomingSide string, incomingPrice, restingPrice *big.Rat) bool {
	if incomingSide == "buy" {
		return incomingPrice.Cmp(restingPrice) >= 0
	}
	return incomingPrice.Cmp(restingPrice) <= 0
}

func shouldSkipRestingOrderForBuy(fillQuote, incomingAmountInRemaining *big.Int) bool {
	if fillQuote == nil || incomingAmountInRemaining == nil {
		return false
	}
	return fillQuote.Cmp(incomingAmountInRemaining) > 0
}

func matchOrder(ctx context.Context, db *sql.DB, orderID int64) ([]StoredFill, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var incoming matchOrderRow
	var pairID, network, expiration, orderType string
	err = tx.QueryRowContext(ctx, `SELECT id, order_hash, maker, side, price, amount, COALESCE(filled_amount, '0'), amount_in, amount_out_min, token_in, token_out, pair_id, network, expiration::text, order_type FROM dex_orders WHERE id=$1 FOR UPDATE`, orderID).Scan(
		&incoming.ID, &incoming.OrderHash, &incoming.Maker, &incoming.Side, &incoming.Price, &incoming.Amount, &incoming.FilledAmount, &incoming.AmountIn, &incoming.AmountOutMin, &incoming.TokenIn, &incoming.TokenOut, &pairID, &network, &expiration, &orderType)
	if err != nil {
		return nil, err
	}
	_ = expiration

	// Fetch quote token info for this pair
	var quoteTokenSymbol, quoteTokenAddress sql.NullString
	// Keep metadata enrichment isolated: a missing venue table must never abort
	// the order-matching transaction.
	_, _ = tx.ExecContext(ctx, `SAVEPOINT quote_metadata_lookup`)
	if network == "solana" {
		// For Solana (Meteora DLMM), token_x is quote
		_ = tx.QueryRowContext(ctx, `SELECT token_x_symbol, token_x_mint FROM meteora_dlmm_pools WHERE address=$1`, pairID).Scan(&quoteTokenSymbol, &quoteTokenAddress)
	} else if network == "bsc" || network == "base" || network == "robinhood" {
		// EVM pools use different tables per chain/venue. Always persist the
		// actual quote token from the pool, including Robinhood, so volume is
		// independent of whether the UI reverses the displayed pair.
		_ = tx.QueryRowContext(ctx, `
			SELECT quote_symbol, quote_mint FROM pools WHERE address=$1
			UNION ALL SELECT token1_symbol, token1 FROM bsc_pancakeswap_v2_pools WHERE address=$1
			UNION ALL SELECT token1_symbol, token1 FROM bsc_pancakeswap_v3_pools WHERE address=$1
			UNION ALL SELECT token1_symbol, token1 FROM bsc_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT currency1_symbol, currency1 FROM bsc_uniswap_v4_pools WHERE address=$1
			UNION ALL SELECT token1_symbol, token1 FROM base_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT currency1_symbol, currency1 FROM base_uniswap_v4_pools WHERE address=$1
			UNION ALL SELECT currency1_symbol, currency1 FROM aerodrome_slipstream_pools WHERE address=$1
			UNION ALL SELECT token1_symbol, token1 FROM robinhood_uniswap_v2_pools WHERE address=$1
			UNION ALL SELECT token1_symbol, token1 FROM robinhood_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT currency1_symbol, currency1 FROM robinhood_uniswap_v4_pools WHERE address=$1
			LIMIT 1`, pairID).Scan(&quoteTokenSymbol, &quoteTokenAddress)
	}
	if !quoteTokenSymbol.Valid && !quoteTokenAddress.Valid {
		_, _ = tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT quote_metadata_lookup`)
		// The signed order direction is authoritative even when pool metadata is
		// unavailable: buy.quote = tokenIn, sell.quote = tokenOut.
		if incoming.Side == "buy" {
			quoteTokenAddress = sql.NullString{String: incoming.TokenIn, Valid: incoming.TokenIn != ""}
		} else {
			quoteTokenAddress = sql.NullString{String: incoming.TokenOut, Valid: incoming.TokenOut != ""}
		}
	}
	_, _ = tx.ExecContext(ctx, `RELEASE SAVEPOINT quote_metadata_lookup`)

	if incoming.Side != "buy" && incoming.Side != "sell" {
		return nil, fmt.Errorf("unsupported order side")
	}
	isMarket := orderType == "market"
	incomingPrice, err := parsePrice(incoming.Price)
	if err != nil && !isMarket {
		return nil, err
	}
	// Market orders match any resting price — use a sentinel that always crosses.
	if isMarket || incomingPrice == nil {
		if incoming.Side == "buy" {
			incomingPrice = new(big.Rat).SetFloat64(1e30) // effectively ∞ bid
		} else {
			incomingPrice = new(big.Rat).SetFloat64(1e-30) // effectively 0 ask
		}
	}
	incomingAmount, err := parseBigInt(incoming.Amount)
	if err != nil {
		return nil, err
	}
	incomingFilled, err := parseBigInt(incoming.FilledAmount)
	if err != nil {
		return nil, err
	}
	remaining := new(big.Int).Sub(incomingAmount, incomingFilled)
	if remaining.Sign() <= 0 {
		return nil, tx.Commit()
	}

	var marketReferencePrice *big.Rat
	var hasMarketReferencePrice bool
	if isMarket {
		marketReferencePrice, hasMarketReferencePrice = marketOrderReferencePrice(tx, pairID)
	}

	priceFilter := `AND COALESCE(price, '') <> ''`
	if isMarket {
		priceFilter = `AND (COALESCE(price, '') <> '' OR order_type = 'market')`
	}
	orderQuery := `SELECT id, order_hash, maker, side, order_type, price, amount, COALESCE(filled_amount, '0'), amount_in, amount_out_min, token_in, token_out
		FROM dex_orders
		WHERE pair_id=$1 AND LOWER(network)=LOWER($2) AND side=$3 AND status IN ('pending','open','partial')
		` + priceFilter + `
		AND expiration > EXTRACT(EPOCH FROM NOW()) AND maker <> $4
		ORDER BY price ` + map[string]string{"buy": "ASC", "sell": "DESC"}[incoming.Side] + `, created_at ASC, id ASC FOR UPDATE SKIP LOCKED`
	oppositeSide := "sell"
	if incoming.Side == "sell" {
		oppositeSide = "buy"
	}
	rows, err := tx.QueryContext(ctx, orderQuery, pairID, network, oppositeSide, incoming.Maker)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	restingOrders := make([]matchOrderRow, 0)
	for rows.Next() {
		var resting matchOrderRow
		if err := rows.Scan(&resting.ID, &resting.OrderHash, &resting.Maker, &resting.Side, &resting.OrderType, &resting.Price, &resting.Amount, &resting.FilledAmount, &resting.AmountIn, &resting.AmountOutMin, &resting.TokenIn, &resting.TokenOut); err != nil {
			return nil, err
		}
		if strings.TrimSpace(resting.Price) == "" && strings.TrimSpace(resting.OrderType) != "market" {
			continue
		}
		restingOrders = append(restingOrders, resting)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	fills := make([]StoredFill, 0)
	for _, resting := range restingOrders {
		if remaining.Sign() <= 0 {
			break
		}
		var restingPrice *big.Rat
		if strings.TrimSpace(resting.Price) != "" {
			restingPrice, err = parsePrice(resting.Price)
			if err != nil {
				return nil, err
			}
		} else if isMarket && hasMarketReferencePrice {
			restingPrice = marketReferencePrice
		} else {
			continue
		}
		if !pricesCross(incoming.Side, incomingPrice, restingPrice) {
			break
		}
		restingAmount, err := parseBigInt(resting.Amount)
		if err != nil {
			return nil, err
		}
		restingFilled, err := parseBigInt(resting.FilledAmount)
		if err != nil {
			return nil, err
		}
		restingRemaining := new(big.Int).Sub(restingAmount, restingFilled)
		if restingRemaining.Sign() <= 0 {
			continue
		}
		fillAmount := new(big.Int).Set(remaining)
		if restingRemaining.Cmp(fillAmount) < 0 {
			fillAmount.Set(restingRemaining)
		}
		// Settlement retries the original fill. Do not create another fill for the
		// same order pair while that original record is pending or failed.
		var existingFill int64
		if err := tx.QueryRowContext(ctx,
			`SELECT id FROM dex_fills WHERE maker_order_id=$1 AND taker_order_id=$2 LIMIT 1`,
			resting.ID, incoming.ID).Scan(&existingFill); err == nil {
			continue
		} else if err != sql.ErrNoRows {
			return nil, err
		}
		// Quote amount must be proportional to restingRemaining (what's actually left),
		// not restingAmount (the original total). Using restingAmount overstates the
		// quote on already-partially-filled resting orders.
		var quoteBase *big.Int
		if resting.Side == "sell" {
			quoteBase, err = parseBigInt(resting.AmountOutMin)
		} else {
			quoteBase, err = parseBigInt(resting.AmountIn)
		}
		if err != nil {
			return nil, err
		}
		// Scale: quoteBase * fillAmount / restingRemaining
		fillQuote := proportionalAmount(quoteBase, fillAmount, restingRemaining)
		if fillQuote.Sign() == 0 && marketReferencePrice != nil && fillAmount.Sign() > 0 {
			fillQuote = computeFillQuoteFromReferencePrice(fillAmount, marketReferencePrice)
		}

		// Affordability check: only applies to BUY orders.
		// For buy orders, amountIn = quote token and fillQuote is also in quote token.
		// If the current resting order is too expensive for the buyer's remaining budget,
		// reduce the fill to the affordable amount instead of cancelling the whole scan.
		if incoming.Side == "buy" {
			incomingAmountIn, ainErr := parseBigInt(incoming.AmountIn)
			if ainErr == nil && incomingAmountIn.Sign() > 0 {
				incomingAmountConsumed := proportionalAmount(incomingAmountIn, incomingFilled, incomingAmount)
				incomingAmountInRemaining := new(big.Int).Sub(incomingAmountIn, incomingAmountConsumed)
				if shouldSkipRestingOrderForBuy(fillQuote, incomingAmountInRemaining) && quoteBase.Sign() > 0 {
					budgetCap := new(big.Int).Mul(incomingAmountInRemaining, restingRemaining)
					budgetCap.Quo(budgetCap, quoteBase)
					if budgetCap.Sign() <= 0 {
						continue
					}
					if budgetCap.Cmp(fillAmount) < 0 {
						fillAmount = budgetCap
						fillQuote = proportionalAmount(quoteBase, fillAmount, restingRemaining)
						if fillQuote.Sign() == 0 && marketReferencePrice != nil && fillAmount.Sign() > 0 {
							fillQuote = computeFillQuoteFromReferencePrice(fillAmount, marketReferencePrice)
						}
					}
				}
			}
		}
		if fillAmount.Sign() <= 0 {
			continue
		}
		newRestingFilled := new(big.Int).Add(restingFilled, fillAmount)
		restingStatus := "partial"
		if newRestingFilled.Cmp(restingAmount) >= 0 {
			restingStatus = "filled"
		}
		if _, err = tx.ExecContext(ctx, `UPDATE dex_orders SET filled_amount=$1, status=$2, updated_at=NOW() WHERE id=$3`, newRestingFilled.String(), restingStatus, resting.ID); err != nil {
			return nil, err
		}
		incomingFilled.Add(incomingFilled, fillAmount)
		remaining.Sub(remaining, fillAmount)
		fillPrice := strings.TrimSpace(resting.Price)
		pairFlipped := pairShouldFlipDisplay(tx, pairID)
		if fillPrice == "" && fillQuote.Sign() > 0 && fillAmount.Sign() > 0 {
			fillPrice = computeFillPriceFromQuoteAmount(fillAmount, fillQuote, pairFlipped)
		}
		// Create fill with quote token info
		fill := StoredFill{
			MakerOrderID: resting.ID,
			TakerOrderID: incoming.ID,
			PairID:       pairID,
			Price:        fillPrice,
			Amount:       fillAmount.String(),
			AmountQuote:  fillQuote.String(),
			Maker:        resting.Maker,
			Taker:        incoming.Maker,
			Side:         incoming.Side,
		}
		if quoteTokenSymbol.Valid {
			fill.QuoteTokenSymbol = quoteTokenSymbol.String
		}
		if quoteTokenAddress.Valid {
			fill.QuoteTokenAddress = quoteTokenAddress.String
		}
		fills = append(fills, fill)
	}
	incomingStatus := "open"
	if incomingFilled.Sign() > 0 {
		incomingStatus = "partial"
	}
	if remaining.Sign() == 0 {
		incomingStatus = "filled"
	}
	if _, err = tx.ExecContext(ctx, `UPDATE dex_orders SET filled_amount=$1, status=$2, updated_at=NOW() WHERE id=$3`, incomingFilled.String(), incomingStatus, incoming.ID); err != nil {
		return nil, err
	}
	for index := range fills {
		fill := &fills[index]
		err = tx.QueryRowContext(ctx, `INSERT INTO dex_fills (maker_order_id,taker_order_id,pair_id,price,amount,amount_quote,maker,taker,quote_token_symbol,quote_token_address) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id,created_at::text`, fill.MakerOrderID, fill.TakerOrderID, fill.PairID, fill.Price, fill.Amount, fill.AmountQuote, fill.Maker, fill.Taker, fill.QuoteTokenSymbol, fill.QuoteTokenAddress).Scan(&fill.ID, &fill.CreatedAt)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return fills, nil
}

func createOrderHandler(db *sql.DB, hub *pairHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request CreateOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_order"})
			return
		}
		if request.OrderHash == "" || request.Maker == "" || request.PairID == "" || request.Signature == "" || request.TokenIn == "" || request.TokenOut == "" {
			log.Printf("[api-orders] missing field request=%+v", request)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing_order_fields"})
			return
		}
		log.Printf("[api-orders] incoming request network=%s pair=%s maker=%s side=%s orderType=%s signature=%q", request.Network, request.PairID, request.Maker, request.Side, request.OrderType, request.Signature)
		// Normalize network to lowercase so matching always works regardless of
		// what the frontend sends ("Base", "BSC", "base", "bsc", etc.)
		request.Network = strings.ToLower(strings.TrimSpace(request.Network))
		if request.Network == "robinhood chain" || request.Network == "robinhood mainnet" {
			request.Network = "robinhood"
		}
		if request.Network == "" {
			request.Network = "bsc" // safe fallback
		}
		if request.Network == "solana" {
			if err := verifySolanaOrderSignature(request); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
		}
		// Validate order type.
		validOrderTypes := map[string]bool{"limit": true, "market": true, "stop_loss": true, "take_profit": true, "ladder": true}
		if !validOrderTypes[request.OrderType] {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_order_type"})
			return
		}
		// stop_loss and take_profit require a trigger_price.
		isConditional := request.OrderType == "stop_loss" || request.OrderType == "take_profit"
		if isConditional && request.TriggerPrice == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "trigger_price_required_for_conditional_orders"})
			return
		}
		if request.OrderType == "market" {
			sanitizeMarketOrderRequest(&request)
		}
		if isConditional {
			if request.ExecutionType == "" {
				request.ExecutionType = "limit"
			}
			if request.ExecutionType != "market" && request.ExecutionType != "limit" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_conditional_execution_type"})
				return
			}
			sanitizeConditionalOrderRequest(&request)
		}
		var order StoredOrder
		err := db.QueryRowContext(r.Context(), `INSERT INTO dex_orders
		(order_hash,maker,pair_id,network,side,order_type,price,trigger_price,execution_type,amount,amount_in,amount_out_min,token_in,token_out,token_in_account,token_out_account,receiver,signature,expiration,nonce,salt,ladder_levels,ladder_price_start,ladder_price_end,post_only)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
		RETURNING `+orderFields,
			request.OrderHash, request.Maker, request.PairID, request.Network, request.Side, request.OrderType,
			request.Price, request.TriggerPrice, request.ExecutionType, request.Amount, request.AmountIn, request.AmountOutMin,
			request.TokenIn, request.TokenOut, request.TokenInAccount, request.TokenOutAccount, request.Receiver, request.Signature,
			request.Expiration, request.Nonce, request.Salt,
			request.LadderLevels, request.LadderPriceStart, request.LadderPriceEnd,
			request.PostOnly,
		).Scan(
			&order.ID, &order.OrderHash, &order.Maker, &order.PairID, &order.Network, &order.Side, &order.OrderType,
			&order.Price, &order.TriggerPrice, &order.Amount, &order.FilledAmount, &order.AmountIn, &order.AmountOutMin,
			&order.TokenIn, &order.TokenOut, &order.TokenInAccount, &order.TokenOutAccount, &order.Receiver, &order.Signature,
			&order.Expiration, &order.Nonce, &order.Salt, &order.Status, &order.CreatedAt,
			&order.LadderLevels, &order.LadderPriceStart, &order.LadderPriceEnd, &order.LadderLevel, &order.LadderParentHash, &order.LadderTotalAmount, &order.IsLadder, &order.PostOnly,
		)
		if err != nil {
			if strings.Contains(err.Error(), "duplicate key") {
				writeJSON(w, http.StatusConflict, map[string]string{"error": "order_already_exists"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order_persist_failed"})
			return
		}
		// Conditional orders (stop_loss / take_profit) are stored pending and matched
		// only when the trigger loop fires — skip immediate matching entirely.
		if isConditional {
			hub.broadcast(map[string]any{"type": "order_update", "order": order})
			writeJSON(w, http.StatusCreated, order)
			return
		}
		if request.LadderLevels > 1 {
			_, err = db.ExecContext(r.Context(), `UPDATE dex_orders SET status='filled', is_ladder=TRUE WHERE id=$1`, order.ID)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "ladder_parent_update_failed"})
				return
			}
			order.Status = "filled"
			order.IsLadder = true
			baseTotal := new(big.Int)
			baseTotal.SetString(request.LadderAmount, 10)
			if baseTotal.Sign() <= 0 {
				baseTotal.SetString(request.Amount, 10)
			}
			// Compute per-level base amount. Integer-divide evenly across levels;
			// give any rounding remainder to the last level so no dust is lost.
			nLevels := int64(request.LadderLevels)
			perLevel := new(big.Int).Quo(baseTotal, big.NewInt(nLevels))

			start, _ := strconv.ParseFloat(request.LadderPriceStart, 64)
			end, _ := strconv.ParseFloat(request.LadderPriceEnd, 64)

			// Pre-parse totals for proportional scaling.
			// For SELL ladders: amountIn = base token (USDC), amountOutMin = quote token (WETH)
			//   → both scale uniformly with perLevel/baseTotal (same base units).
			// For BUY ladders: amountIn = quote token (WETH), amountOutMin = base token (USDC)
			//   → amountOutMin scales uniformly (it's in base); amountIn must be
			//     computed per-level from levelPrice because each level has a different price.
			totalAmountIn, ainErr := parseBigInt(request.AmountIn)
			totalAmountOutMin, aomErr := parseBigInt(request.AmountOutMin)

			for level := 1; level <= request.LadderLevels; level++ {
				// Price interpolation: level 1 = priceStart, level N = priceEnd.
				// Guard levels=1 (would divide by zero in (levels-1)).
				var price float64
				if request.LadderLevels == 1 {
					price = start
				} else {
					price = start + (end-start)*float64(level-1)/float64(request.LadderLevels-1)
				}
				priceText := strconv.FormatFloat(price, 'f', 8, 64)
				childHash := fmt.Sprintf("%s-level-%d", request.OrderHash, level)

				// Last level absorbs any rounding dust so that
				// sum(childAmount across all levels) == baseTotal exactly.
				childAmount := new(big.Int).Set(perLevel)
				if level == request.LadderLevels {
					// remainder = baseTotal - perLevel*(levels-1)
					consumed := new(big.Int).Mul(perLevel, big.NewInt(nLevels-1))
					childAmount.Sub(baseTotal, consumed)
				}

				// Compute per-level amountIn and amountOutMin.
				childAmountIn := request.AmountIn
				childAmountOutMin := request.AmountOutMin
				if request.Side == "sell" {
					// SELL: amountIn = base token being sold — scales uniformly per level.
					if ainErr == nil && totalAmountIn.Sign() > 0 && baseTotal.Sign() > 0 {
						childAmountIn = proportionalAmount(totalAmountIn, childAmount, baseTotal).String()
					}
					// SELL: amountOutMin = quote token to receive.
					// totalAmountOutMin was computed at priceStart on the frontend.
					// Each level needs its OWN price so we scale by (levelPrice / priceStart):
					//   childAmountOutMin = (totalAmountOutMin * childAmount / baseTotal) * (levelPrice / priceStart)
					// This correctly gives more WETH for higher-priced levels.
					if aomErr == nil && totalAmountOutMin.Sign() > 0 && baseTotal.Sign() > 0 && start > 0 {
						// base portion = totalAmountOutMin * childAmount / baseTotal
						basePortion := proportionalAmount(totalAmountOutMin, childAmount, baseTotal)
						// scale by levelPrice / priceStart using big.Float for precision
						fBase := new(big.Float).SetInt(basePortion)
						fLevel := big.NewFloat(price)
						fStart := big.NewFloat(start)
						scaled := new(big.Float).Mul(fBase, new(big.Float).Quo(fLevel, fStart))
						ri, _ := scaled.Int(nil)
						if ri.Sign() > 0 {
							childAmountOutMin = ri.String()
						} else {
							childAmountOutMin = basePortion.String()
						}
					}
				} else {
					// BUY: amountIn = quote token paid for this level.
					// Each level has a different price so we derive amountIn from price:
					//   childAmountIn = childAmount(base) * levelPrice  (quote per base)
					// amountOutMin = base tokens to receive = childAmount (uniform).
					if price > 0 && childAmount.Sign() > 0 {
						// Compute quote using the same 8-decimal fixed-point as the contract:
						//   quoteRaw = childAmount * priceFixed / 1e8  (adjusted for decimals)
						// We don't know decimals here, but they're handled by the contract.
						// Mirror the formula the frontend uses: amountQuote = ceil(qty * price * 10^quoteDecimals).
						// Without decimals we use the ratio: childAmountIn = totalAmountIn * childAmount / totalAmount(base).
						// This is valid because totalAmountIn was computed as total_base * price on the frontend.
						if ainErr == nil && totalAmountIn.Sign() > 0 && baseTotal.Sign() > 0 {
							// For uniform-price ladders this equals uniform split.
							// For variable-price ladders this is an approximation — the exact
							// per-level quote is proportional to (childAmount * levelPrice).
							// We use the price ratio to scale:
							//   childAmountIn = totalAmountIn * (childAmount * levelPrice) / (baseTotal * avgPrice)
							// avgPrice ≈ (start+end)/2 — use it to de-normalize.
							avgPrice := (start + end) / 2.0
							if avgPrice > 0 {
								// numerator = totalAmountIn * childAmount * levelPrice
								// denominator = baseTotal * avgPrice
								// Use big.Float for intermediate precision, then truncate.
								fTotal := new(big.Float).SetInt(totalAmountIn)
								fChild := new(big.Float).SetInt(childAmount)
								fBase := new(big.Float).SetInt(baseTotal)
								fPrice := big.NewFloat(price)
								fAvg := big.NewFloat(avgPrice)
								// childAmountIn = totalAmountIn * (childAmount/baseTotal) * (levelPrice/avgPrice)
								ratio := new(big.Float).Mul(new(big.Float).Quo(fChild, fBase), new(big.Float).Quo(fPrice, fAvg))
								result := new(big.Float).Mul(fTotal, ratio)
								// Ceil: add 0.999... and truncate
								result.Add(result, big.NewFloat(0.9999))
								ri, _ := result.Int(nil)
								childAmountIn = ri.String()
							} else {
								childAmountIn = proportionalAmount(totalAmountIn, childAmount, baseTotal).String()
							}
						}
					}
					// amountOutMin for buy = base tokens to receive = childAmount
					childAmountOutMin = childAmount.String()
				}

				var childID int64
				err = db.QueryRowContext(r.Context(), `INSERT INTO dex_orders (order_hash,maker,pair_id,network,side,order_type,price,amount,amount_in,amount_out_min,token_in,token_out,token_in_account,token_out_account,receiver,signature,expiration,nonce,salt,ladder_levels,ladder_price_start,ladder_price_end,ladder_level,ladder_parent_hash,ladder_total_amount,is_ladder) VALUES ($1,$2,$3,$4,$5,'ladder',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25) RETURNING id`,
					childHash, request.Maker, request.PairID, request.Network, request.Side,
					priceText, childAmount.String(), childAmountIn, childAmountOutMin,
					request.TokenIn, request.TokenOut, request.TokenInAccount, request.TokenOutAccount, request.Receiver, request.Signature,
					request.Expiration, request.Nonce, request.Salt,
					request.LadderLevels, request.LadderPriceStart, request.LadderPriceEnd,
					level, request.OrderHash, baseTotal.String(), true,
				).Scan(&childID)
				if err != nil {
					writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "ladder_child_create_failed"})
					return
				}
				fills, matchErr := matchOrder(r.Context(), db, childID)
				if matchErr != nil {
					log.Printf("ladder order %d matching failed: %v", childID, matchErr)
				} else {
					for _, fill := range fills {
						hub.broadcast(map[string]any{"type": "order_fill", "fill": fill})
						broadcastPairUpdate(r.Context(), db, hub, fill.PairID)
					}
				}
				child := order
				child.OrderHash = childHash
				child.OrderType = "ladder"
				child.Price = priceText
				child.Amount = childAmount.String()
				child.AmountIn = childAmountIn
				child.AmountOutMin = childAmountOutMin
				child.Status = "open"
				child.LadderLevel = level
				child.LadderParentHash = request.OrderHash
				child.LadderTotalAmount = baseTotal.String()
				child.IsLadder = true
				var childStatus, childFilled string
				if err := db.QueryRowContext(r.Context(), `SELECT status, filled_amount FROM dex_orders WHERE id=$1`, childID).Scan(&childStatus, &childFilled); err == nil {
					child.Status = childStatus
					child.FilledAmount = childFilled
				}
				hub.broadcast(map[string]any{"type": "order_update", "order": child})
				broadcastPairUpdate(r.Context(), db, hub, child.PairID)
			}
			writeJSON(w, http.StatusCreated, order)
			return
		}
		// ── Post Only check ───────────────────────────────────────────────────
		// Post Only orders must not match as a taker immediately.
		// Before running matchOrder, check whether any resting opposite-side
		// order would cross this order's price. If yes, cancel the just-inserted
		// order and return 409 — the user must adjust price to avoid crossing.
		if request.PostOnly && request.OrderType == "limit" {
			oppSide := "sell"
			if request.Side == "sell" {
				oppSide = "buy"
			}
			sortDir := map[string]string{"buy": "ASC", "sell": "DESC"}[request.Side]
			var bestRestingPrice string
			checkErr := db.QueryRowContext(r.Context(),
				`SELECT price FROM dex_orders
				 WHERE pair_id=$1 AND network=$2 AND side=$3
				   AND status IN ('pending','open','partial')
				   AND expiration > EXTRACT(EPOCH FROM NOW())
				   AND maker <> $4
				 ORDER BY price `+sortDir+` LIMIT 1`,
				request.PairID, request.Network, oppSide, request.Maker,
			).Scan(&bestRestingPrice)
			if checkErr == nil && bestRestingPrice != "" {
				incomingP, ipErr := parsePrice(request.Price)
				restingP, rpErr := parsePrice(bestRestingPrice)
				if ipErr == nil && rpErr == nil && pricesCross(request.Side, incomingP, restingP) {
					// Would fill immediately — cancel the order and reject.
					_, _ = db.ExecContext(r.Context(),
						`UPDATE dex_orders SET status='cancelled' WHERE id=$1`, order.ID)
					writeJSON(w, http.StatusConflict, map[string]string{
						"error":            "post_only_would_fill",
						"bestRestingPrice": bestRestingPrice,
					})
					return
				}
			}
		}

		fills, matchErr := matchOrder(r.Context(), db, order.ID)
		if matchErr != nil {
			log.Printf("order %d matching failed: %v", order.ID, matchErr)
		} else {
			for _, fill := range fills {
				hub.broadcast(map[string]any{"type": "order_fill", "fill": fill})
				broadcastPairUpdate(r.Context(), db, hub, fill.PairID)
			}
		}
		if err := db.QueryRowContext(r.Context(), `SELECT status, filled_amount FROM dex_orders WHERE id=$1`, order.ID).Scan(&order.Status, &order.FilledAmount); err != nil {
			log.Printf("order %d refresh after matching failed: %v", order.ID, err)
		}
		hub.broadcast(map[string]any{"type": "order_update", "order": order})
		broadcastPairUpdate(r.Context(), db, hub, order.PairID)
		writeJSON(w, http.StatusCreated, order)
	}
}

func listOrdersHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := `SELECT id,order_hash,maker,pair_id,network,side,order_type,price,trigger_price,amount,filled_amount,amount_in,amount_out_min,token_in,token_out,receiver,signature,expiration,nonce,salt,status,created_at,ladder_levels,ladder_price_start,ladder_price_end,ladder_level,ladder_parent_hash,ladder_total_amount,is_ladder,post_only FROM dex_orders WHERE 1=1`
		args := []any{}
		isHistory := strings.EqualFold(r.URL.Query().Get("history"), "true")
		if !isHistory {
			query += " AND status NOT IN ('expired','cancelled')"
		}
		if ladder := strings.TrimSpace(r.URL.Query().Get("ladder")); ladder == "true" {
			query += " AND is_ladder = TRUE AND ladder_level > 0"
		} else if ladder == "false" {
			query += " AND is_ladder = FALSE"
		} else {
			query += " AND NOT (is_ladder = TRUE AND ladder_level = 0)"
		}
		if pairID := strings.TrimSpace(r.URL.Query().Get("pairId")); pairID != "" {
			args = append(args, pairID)
			query += fmt.Sprintf(" AND pair_id = $%d", len(args))
		}
		if maker := strings.TrimSpace(r.URL.Query().Get("maker")); maker != "" {
			args = append(args, maker)
			query += fmt.Sprintf(" AND lower(maker) = lower($%d)", len(args))
		}
		if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
			args = append(args, status)
			query += fmt.Sprintf(" AND lower(status)=lower($%d)", len(args))
		}
		if network := strings.TrimSpace(r.URL.Query().Get("network")); network != "" && !strings.EqualFold(network, "all") {
			args = append(args, network)
			query += fmt.Sprintf(" AND lower(network) = lower($%d)", len(args))
		}
		query += " ORDER BY created_at DESC LIMIT 100"
		rows, err := db.QueryContext(r.Context(), query, args...)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "orders_query_failed"})
			return
		}
		defer rows.Close()
		orders := make([]StoredOrder, 0)
		for rows.Next() {
			var order StoredOrder
			if err := rows.Scan(&order.ID, &order.OrderHash, &order.Maker, &order.PairID, &order.Network, &order.Side, &order.OrderType,
				&order.Price, &order.TriggerPrice, &order.Amount, &order.FilledAmount, &order.AmountIn, &order.AmountOutMin, &order.TokenIn, &order.TokenOut,
				&order.Receiver, &order.Signature, &order.Expiration, &order.Nonce, &order.Salt, &order.Status, &order.CreatedAt, &order.LadderLevels, &order.LadderPriceStart, &order.LadderPriceEnd, &order.LadderLevel, &order.LadderParentHash, &order.LadderTotalAmount, &order.IsLadder, &order.PostOnly); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "order_decode_failed"})
				return
			}
			orders = append(orders, order)
		}

		// For history orders, enrich market orders (price="") with their actual
		// fill price from dex_fills so the UI can show the execution price.
		// Strategy: for any order with no price set, look up the most recent fill
		// where it was either maker or taker.
		if isHistory && len(orders) > 0 {
			marketOrderIDs := make([]int64, 0)
			for _, o := range orders {
				if strings.TrimSpace(o.Price) == "" || o.Price == "0" {
					marketOrderIDs = append(marketOrderIDs, o.ID)
				}
			}
			if len(marketOrderIDs) > 0 {
				// Use a UNION ALL to get fill prices from both maker and taker sides.
				// Each side gets its own set of positional params: $1..$N for maker,
				// $(N+1)..$2N for taker.
				placeholders1 := make([]string, len(marketOrderIDs))
				placeholders2 := make([]string, len(marketOrderIDs))
				fillArgs := make([]any, len(marketOrderIDs)*2)
				for i, id := range marketOrderIDs {
					placeholders1[i] = fmt.Sprintf("$%d", i+1)
					placeholders2[i] = fmt.Sprintf("$%d", len(marketOrderIDs)+i+1)
					fillArgs[i] = id
					fillArgs[len(marketOrderIDs)+i] = id
				}
				fillQuery := fmt.Sprintf(
					`SELECT maker_order_id AS o_id, price FROM dex_fills
					 WHERE maker_order_id IN (%s) AND price <> '' AND price <> '0'
					 UNION ALL
					 SELECT taker_order_id AS o_id, price FROM dex_fills
					 WHERE taker_order_id IN (%s) AND price <> '' AND price <> '0'
					 ORDER BY o_id, price DESC`,
					strings.Join(placeholders1, ","),
					strings.Join(placeholders2, ","),
				)
				fillRows, fillErr := db.QueryContext(r.Context(), fillQuery, fillArgs...)
				if fillErr == nil {
					fillPriceMap := make(map[int64]string)
					for fillRows.Next() {
						var oid int64
						var fp string
						if scanErr := fillRows.Scan(&oid, &fp); scanErr == nil {
							// Keep the first (most recent) price per order
							if _, exists := fillPriceMap[oid]; !exists {
								fillPriceMap[oid] = fp
							}
						}
					}
					_ = fillRows.Close()
					for i := range orders {
						if fp, ok := fillPriceMap[orders[i].ID]; ok {
							orders[i].FillPrice = fp
						}
					}
				} else {
					log.Printf("[orders] fill price enrichment failed: %v", fillErr)
				}
			}
		}

		writeJSON(w, http.StatusOK, orders)
	}
}

func listFillsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := `SELECT f.id,f.maker_order_id,f.taker_order_id,f.pair_id,f.price,f.amount,f.amount_quote,f.maker,f.taker,f.created_at::text,f.settlement_status,f.settlement_attempts,COALESCE(f.last_attempted_at::text,''),f.tx_hash,COALESCE(f.tx_hash_buy,''),COALESCE(f.tx_hash_sell,''),f.last_error,t.side,t.network,COALESCE(f.quote_token_symbol,''),COALESCE(f.quote_token_address,'') FROM dex_fills f JOIN dex_orders t ON t.id=f.taker_order_id WHERE 1=1`
		args := make([]any, 0, 2)
		if pairID := strings.TrimSpace(r.URL.Query().Get("pairId")); pairID != "" {
			args = append(args, pairID)
			query += fmt.Sprintf(" AND f.pair_id=$%d", len(args))
		}
		if maker := strings.TrimSpace(r.URL.Query().Get("maker")); maker != "" {
			args = append(args, maker)
			query += fmt.Sprintf(" AND (lower(maker)=lower($%d) OR lower(taker)=lower($%d))", len(args), len(args))
		}
		if status := strings.TrimSpace(r.URL.Query().Get("settlementStatus")); status != "" {
			args = append(args, status)
			query += fmt.Sprintf(" AND lower(f.settlement_status)=lower($%d)", len(args))
		}
		limit := 200
		if value, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && value > 0 && value <= 200 {
			limit = value
		}
		query += fmt.Sprintf(" ORDER BY f.created_at DESC LIMIT %d", limit)
		rows, err := db.QueryContext(r.Context(), query, args...)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "fills_query_failed"})
			return
		}
		defer rows.Close()
		fills := make([]StoredFill, 0)
		for rows.Next() {
			var fill StoredFill
			if err := rows.Scan(&fill.ID, &fill.MakerOrderID, &fill.TakerOrderID, &fill.PairID, &fill.Price, &fill.Amount, &fill.AmountQuote, &fill.Maker, &fill.Taker, &fill.CreatedAt, &fill.SettlementStatus, &fill.SettlementAttempts, &fill.LastAttemptedAt, &fill.TxHash, &fill.TxHashBuy, &fill.TxHashSell, &fill.LastError, &fill.Side, &fill.Network, &fill.QuoteTokenSymbol, &fill.QuoteTokenAddress); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "fill_decode_failed"})
				return
			}
			fills = append(fills, fill)
		}
		if err := rows.Err(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "fills_read_failed"})
			return
		}
		writeJSON(w, http.StatusOK, fills)
	}
}

type rowScanner interface{ Scan(dest ...any) error }

const orderFields = `id,order_hash,maker,pair_id,network,side,order_type,price,trigger_price,amount,filled_amount,amount_in,amount_out_min,token_in,token_out,token_in_account,token_out_account,receiver,signature,expiration,nonce,salt,status,created_at::text,ladder_levels,ladder_price_start,ladder_price_end,ladder_level,ladder_parent_hash,ladder_total_amount,is_ladder,post_only`

func scanOrder(scanner rowScanner, order *StoredOrder) error {
	return scanner.Scan(&order.ID, &order.OrderHash, &order.Maker, &order.PairID, &order.Network, &order.Side, &order.OrderType, &order.Price, &order.TriggerPrice, &order.Amount, &order.FilledAmount, &order.AmountIn, &order.AmountOutMin, &order.TokenIn, &order.TokenOut, &order.TokenInAccount, &order.TokenOutAccount, &order.Receiver, &order.Signature, &order.Expiration, &order.Nonce, &order.Salt, &order.Status, &order.CreatedAt, &order.LadderLevels, &order.LadderPriceStart, &order.LadderPriceEnd, &order.LadderLevel, &order.LadderParentHash, &order.LadderTotalAmount, &order.IsLadder, &order.PostOnly)
}

func settlementAuthorized(r *http.Request) bool {
	key := strings.TrimSpace(os.Getenv("SETTLEMENT_API_KEY"))
	provided := strings.TrimSpace(r.Header.Get("X-Settlement-Key"))
	if key == "" || provided == "" || len(provided) != len(key) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(provided), []byte(key)) == 1
}

func settlementOwner(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("X-Settlement-Worker"))
}

func loadSettlementPayload(ctx context.Context, db *sql.DB, fillID int64) (SettlementPayload, error) {
	var payload SettlementPayload
	if err := db.QueryRowContext(ctx, `SELECT f.id,f.maker_order_id,f.taker_order_id,f.pair_id,f.price,f.amount,f.amount_quote,f.maker,f.taker,f.created_at::text,f.settlement_status,f.settlement_attempts,COALESCE(f.last_attempted_at::text,''),f.tx_hash,COALESCE(f.tx_hash_buy,''),COALESCE(f.tx_hash_sell,''),f.last_error,t.side,t.network FROM dex_fills f JOIN dex_orders t ON t.id=f.taker_order_id WHERE f.id=$1`, fillID).Scan(&payload.Fill.ID, &payload.Fill.MakerOrderID, &payload.Fill.TakerOrderID, &payload.Fill.PairID, &payload.Fill.Price, &payload.Fill.Amount, &payload.Fill.AmountQuote, &payload.Fill.Maker, &payload.Fill.Taker, &payload.Fill.CreatedAt, &payload.Fill.SettlementStatus, &payload.Fill.SettlementAttempts, &payload.Fill.LastAttemptedAt, &payload.Fill.TxHash, &payload.Fill.TxHashBuy, &payload.Fill.TxHashSell, &payload.Fill.LastError, &payload.Fill.Side, &payload.Fill.Network); err != nil {
		return payload, err
	}
	var makerOrder, takerOrder StoredOrder
	if err := scanOrder(db.QueryRowContext(ctx, `SELECT `+orderFields+` FROM dex_orders WHERE id=$1`, payload.Fill.MakerOrderID), &makerOrder); err != nil {
		return payload, err
	}
	if err := scanOrder(db.QueryRowContext(ctx, `SELECT `+orderFields+` FROM dex_orders WHERE id=$1`, payload.Fill.TakerOrderID), &takerOrder); err != nil {
		return payload, err
	}
	if makerOrder.Side == "buy" {
		payload.BuyOrder = makerOrder
		payload.SellOrder = takerOrder
	} else {
		payload.BuyOrder = takerOrder
		payload.SellOrder = makerOrder
	}
	return payload, nil
}

func pendingSettlementHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !settlementAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "settlement_unauthorized"})
			return
		}
		limit := 25
		if value, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && value > 0 && value <= 500 {
			limit = value
		}
		network := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("network")))
		pairID := strings.TrimSpace(r.URL.Query().Get("pairId"))
		// Expiration is authoritative for both sides of a fill. Expired fills
		// cannot settle on-chain and must never remain retryable in the queue.
		_, _ = db.ExecContext(r.Context(), `
			UPDATE dex_fills f
			SET settlement_status='abandoned', last_error='order_expired', settlement_owner='', settlement_lease_until=NULL
			FROM dex_orders maker, dex_orders taker
			WHERE f.maker_order_id=maker.id AND f.taker_order_id=taker.id
			  AND f.settlement_status IN ('pending','failed','partial','processing')
			  AND LEAST(maker.expiration, taker.expiration) <= EXTRACT(EPOCH FROM NOW())`)
		rows, err := db.QueryContext(r.Context(), `SELECT f.id
			FROM dex_fills f
			JOIN dex_orders maker ON maker.id=f.maker_order_id
			JOIN dex_orders taker ON taker.id=f.taker_order_id
			WHERE f.settlement_status IN ('pending','processing','failed','partial')
			  AND f.settlement_attempts < 5
			  AND maker.expiration > EXTRACT(EPOCH FROM NOW())
			  AND taker.expiration > EXTRACT(EPOCH FROM NOW())
			  AND ($1 = '' OR lower(taker.network) = $1)
			  AND ($2 = '' OR f.pair_id = $2)
			ORDER BY LEAST(maker.expiration, taker.expiration) ASC, f.created_at ASC
			LIMIT $3`, network, pairID, limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "settlement_query_failed"})
			return
		}
		defer rows.Close()
		ids := make([]int64, 0, limit)
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "settlement_decode_failed"})
				return
			}
			ids = append(ids, id)
		}
		payloads := make([]SettlementPayload, 0, len(ids))
		for _, id := range ids {
			payload, err := loadSettlementPayload(r.Context(), db, id)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "settlement_payload_failed"})
				return
			}
			payloads = append(payloads, payload)
		}
		writeJSON(w, http.StatusOK, payloads)
	}
}

func settlementHealthHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !settlementAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "settlement_unauthorized"})
			return
		}
		pairID := strings.TrimSpace(r.URL.Query().Get("pairId"))
		rows, err := db.QueryContext(r.Context(), `SELECT settlement_status, COUNT(*), COALESCE(MIN(created_at)::text, '') FROM dex_fills WHERE ($1 = '' OR pair_id = $1) GROUP BY settlement_status`, pairID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "settlement_health_failed"})
			return
		}
		defer rows.Close()
		counts := make(map[string]map[string]any)
		for rows.Next() {
			var status, oldest string
			var count int64
			if err := rows.Scan(&status, &count, &oldest); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "settlement_health_decode_failed"})
				return
			}
			counts[status] = map[string]any{"count": count, "oldestCreatedAt": oldest}
		}
		if err := rows.Err(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "settlement_health_rows_failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"worker": settlementOwner(r), "pairId": pairID, "statuses": counts, "checkedAt": time.Now().UTC().Format(time.RFC3339)})
	}
}

func settlementFillID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("id"), 10, 64)
}

func claimSettlementHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !settlementAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "settlement_unauthorized"})
			return
		}
		id, err := settlementFillID(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_fill_id"})
			return
		}
		var claimedID int64
		owner := settlementOwner(r)
		if owner == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "settlement_worker_required"})
			return
		}
		err = db.QueryRowContext(r.Context(), `UPDATE dex_fills SET settlement_status='processing', settlement_attempts=settlement_attempts+1, last_attempted_at=NOW(), last_error='', settlement_owner=$2, settlement_lease_until=NOW()+$3::interval WHERE id=$1 AND (settlement_status IN ('pending','failed','partial') OR (settlement_status='processing' AND settlement_lease_until < NOW())) RETURNING id`, id, owner, fmt.Sprintf("%d seconds", int(settlementLeaseDuration.Seconds()))).Scan(&claimedID)
		if err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "fill_not_claimable"})
			return
		}
		payload, err := loadSettlementPayload(r.Context(), db, claimedID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "settlement_payload_failed"})
			return
		}
		writeJSON(w, http.StatusOK, payload)
	}
}

func settlementResultHandler(db *sql.DB, hub *pairHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !settlementAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "settlement_unauthorized"})
			return
		}
		id, err := settlementFillID(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_fill_id"})
			return
		}
		var result SettlementResult
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil || (result.Status != "confirmed" && result.Status != "failed" && result.Status != "submitted" && result.Status != "partial") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_settlement_result"})
			return
		}
		owner := settlementOwner(r)
		if owner == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "settlement_worker_required"})
			return
		}
		if result.Status == "confirmed" {
			// Check if this is a Solana fill with both buy and sell tx hashes
			txHashBuy := ""
			txHashSell := ""
			if result.Metadata != nil {
				if sellSig, ok := result.Metadata["sellSig"].(string); ok {
					txHashSell = sellSig
				}
				if buySig, ok := result.Metadata["buySig"].(string); ok {
					txHashBuy = buySig
				}
			}

			// Update with Solana-specific tx hashes if available
			if txHashBuy != "" && txHashSell != "" {
				var resultRows sql.Result
				resultRows, err = db.ExecContext(r.Context(), `UPDATE dex_fills SET settlement_status='confirmed', tx_hash=$1, tx_hash_buy=$2, tx_hash_sell=$3, last_error='', settled_at=NOW(), settlement_owner='', settlement_lease_until=NULL WHERE id=$4 AND settlement_status='processing' AND settlement_owner=$5`, result.TxHash, txHashBuy, txHashSell, id, owner)
				if err == nil {
					var affected int64
					affected, err = resultRows.RowsAffected()
					if err == nil && affected == 0 {
						err = sql.ErrNoRows
					}
				}
			} else {
				var resultRows sql.Result
				resultRows, err = db.ExecContext(r.Context(), `UPDATE dex_fills SET settlement_status='confirmed', tx_hash=$1, last_error='', settled_at=NOW(), settlement_owner='', settlement_lease_until=NULL WHERE id=$2 AND settlement_status='processing' AND settlement_owner=$3`, result.TxHash, id, owner)
				if err == nil {
					var affected int64
					affected, err = resultRows.RowsAffected()
					if err == nil && affected == 0 {
						err = sql.ErrNoRows
					}
				}
			}

			if err == nil {
				if payload, payloadErr := loadSettlementPayload(r.Context(), db, id); payloadErr == nil {
					payload.Fill.SettlementStatus = "confirmed"
					payload.Fill.TxHash = result.TxHash
					// Broadcast with both tx hashes for Solana
					fillData := map[string]any{
						"type": "settlement_confirmed",
						"fill": payload.Fill,
					}
					if txHashBuy != "" && txHashSell != "" {
						fillData["txHashBuy"] = txHashBuy
						fillData["txHashSell"] = txHashSell
					}
					hub.broadcast(fillData)
					broadcastPairUpdate(r.Context(), db, hub, payload.Fill.PairID)
					go broadcastCandleUpdate(db, hub, payload.Fill.PairID)
				}
			}
		} else if result.Status == "submitted" {
			var resultRows sql.Result
			resultRows, err = db.ExecContext(r.Context(), `UPDATE dex_fills SET settlement_status='processing', tx_hash=$1, last_error=$2, settlement_lease_until=NOW()+$4::interval WHERE id=$3 AND settlement_status='processing' AND settlement_owner=$5`, result.TxHash, result.Error, id, fmt.Sprintf("%d seconds", int(settlementLeaseDuration.Seconds())), owner)
			if err == nil {
				var affected int64
				affected, err = resultRows.RowsAffected()
				if err == nil && affected == 0 {
					err = sql.ErrNoRows
				}
			}
		} else if result.Status == "partial" {
			txHashSell := result.TxHash
			if result.Metadata != nil {
				if sellSig, ok := result.Metadata["sellSig"].(string); ok && sellSig != "" {
					txHashSell = sellSig
				}
			}
			var resultRows sql.Result
			resultRows, err = db.ExecContext(r.Context(), `UPDATE dex_fills SET settlement_status='partial', tx_hash=$1, tx_hash_sell=$2, last_error=$3, settlement_owner='', settlement_lease_until=NULL WHERE id=$4 AND settlement_status='processing' AND settlement_owner=$5`, txHashSell, txHashSell, result.Error, id, owner)
			if err == nil {
				var affected int64
				affected, err = resultRows.RowsAffected()
				if err == nil && affected == 0 {
					err = sql.ErrNoRows
				}
			}
		} else {
			// Keep matched quantities committed. The original fill is the settlement
			// unit and will be retried; reopening orders here creates duplicate fills
			// and can credit one side more than once.
			var resultRows sql.Result
			resultRows, err = db.ExecContext(r.Context(),
				`UPDATE dex_fills SET
					settlement_status = CASE WHEN settlement_attempts >= 5 THEN 'abandoned' ELSE 'failed' END,
					tx_hash=$1, last_error=$2, settlement_owner='', settlement_lease_until=NULL
				WHERE id=$3 AND settlement_status='processing' AND settlement_owner=$4`,
				result.TxHash, result.Error, id, owner)
			if err == nil {
				var affected int64
				affected, err = resultRows.RowsAffected()
				if err == nil && affected == 0 {
					err = sql.ErrNoRows
				}
			}
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "settlement_result_failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": result.Status})
	}
}

func cancelOrderHandler(db *sql.DB, hub *pairHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_order_id"})
			return
		}
		var order StoredOrder
		err = db.QueryRowContext(r.Context(), `UPDATE dex_orders SET status='cancelled', updated_at=NOW() WHERE id=$1 AND status IN ('pending','open','partial') RETURNING id,order_hash,maker,pair_id,network,side,order_type,price,trigger_price,amount,filled_amount,amount_in,amount_out_min,token_in,token_out,receiver,signature,expiration,nonce,salt,status,created_at::text,updated_at::text,ladder_levels,ladder_price_start,ladder_price_end,ladder_level,ladder_parent_hash,ladder_total_amount,is_ladder,post_only`, id).Scan(
			&order.ID, &order.OrderHash, &order.Maker, &order.PairID, &order.Network, &order.Side, &order.OrderType, &order.Price, &order.TriggerPrice, &order.Amount, &order.FilledAmount, &order.AmountIn, &order.AmountOutMin, &order.TokenIn, &order.TokenOut, &order.Receiver, &order.Signature, &order.Expiration, &order.Nonce, &order.Salt, &order.Status, &order.CreatedAt, &order.UpdatedAt, &order.LadderLevels, &order.LadderPriceStart, &order.LadderPriceEnd, &order.LadderLevel, &order.LadderParentHash, &order.LadderTotalAmount, &order.IsLadder, &order.PostOnly)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "order_not_cancellable"})
			return
		}
		log.Printf("[cancel] Broadcasting order_update for cancelled order id=%d maker=%s status=%s", order.ID, order.Maker, order.Status)
		hub.broadcast(map[string]any{"type": "order_update", "order": order})
		broadcastPairUpdate(r.Context(), db, hub, order.PairID)
		writeJSON(w, http.StatusOK, order)
	}
}

func healthHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "database_unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// flipTokens mirrors the frontend FLIP_TOKENS set — pairs whose pool base token
// is one of these are displayed with an inverted price in the UI (e.g. WETH pools
// show USDC/WETH = ~0.00040085, but the pool stores price as USDC-per-WETH = ~2494).
// When building DEX candles from dex_fills, the stored price is the display-oriented
// price that the user signed (already inverted). We need to invert it back to match
// the pool candle orientation so both data sources live on the same price scale.
var flipTokens = map[string]struct{}{
	"SOL": {}, "WETH": {}, "WBNB": {}, "USDT": {}, "USDC": {},
}

// buildDexCandles aggregates confirmed DEX fills for pairID into OHLCV candles
// bucketed by the given interval. The result is sorted ascending by bucket.
// price is the human-readable fill price stored in dex_fills.price.
// volume is the quote-token fill amount normalised to human-readable units.
// Quote amount is orientation-independent, so reversed UI pairs use the same
// volume as the signed/settled order.
func buildDexCandles(
	ctx context.Context,
	db *sql.DB,
	pairID string,
	interval string,
	startTime, endTime int64, // unix seconds; 0 means unbounded
	limit int,
) ([]PairCandle, error) {
	bucketSecs, ok := intervalSeconds[interval]
	if !ok {
		return nil, fmt.Errorf("unsupported interval: %s", interval)
	}

	// Resolve base token decimals so we can convert wei amounts to human units.
	// Also detect if this pair needs fill price pre-inversion (see comment in scan loop below).
	quoteDecimals := 18
	shouldInvert := false
	var bd, qd int
	var baseSymbolStr string
	if err := db.QueryRowContext(ctx, `
		SELECT base_decimals, quote_decimals, COALESCE(base_symbol,'') FROM pools WHERE address=$1
		UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM bsc_pancakeswap_v2_pools WHERE address=$1
		UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM bsc_pancakeswap_v3_pools WHERE address=$1
		UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM bsc_uniswap_v3_pools WHERE address=$1
		UNION ALL SELECT currency0_decimals, currency1_decimals, COALESCE(currency0_symbol,'') FROM bsc_uniswap_v4_pools WHERE address=$1
		UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM base_uniswap_v3_pools WHERE address=$1
		UNION ALL SELECT currency0_decimals, currency1_decimals, COALESCE(currency0_symbol,'') FROM base_uniswap_v4_pools WHERE address=$1
		UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM robinhood_uniswap_v2_pools WHERE address=$1
		UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM robinhood_uniswap_v3_pools WHERE address=$1
		UNION ALL SELECT currency0_decimals, currency1_decimals, COALESCE(currency0_symbol,'') FROM robinhood_uniswap_v4_pools WHERE address=$1
		UNION ALL SELECT token_mint_0_decimals, token_mint_1_decimals, COALESCE(token_mint_0_symbol,'') FROM raydium_pools WHERE address=$1
		UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM raydium_cpmm_pools WHERE address=$1
		UNION ALL SELECT token_a_decimals, token_b_decimals, COALESCE(token_a_symbol,'') FROM meteora_damm_v2_pools WHERE address=$1
		UNION ALL SELECT token_x_decimals, token_y_decimals, COALESCE(token_x_symbol,'') FROM meteora_dlmm_pools WHERE address=$1
		UNION ALL SELECT token_mint_a_decimals, token_mint_b_decimals, COALESCE(token_mint_a_symbol,'') FROM orca_whirlpools WHERE address=$1
		LIMIT 1`, pairID).Scan(&bd, &qd, &baseSymbolStr); err == nil {
		quoteDecimals = qd
		upper := strings.ToUpper(strings.TrimSpace(baseSymbolStr))
		if _, ok := flipTokens[upper]; ok && upper != "" {
			shouldInvert = true
		}
	}

	// Build time-range filters.
	args := []any{pairID, bucketSecs}
	timeFilter := ""
	if startTime > 0 {
		args = append(args, startTime)
		timeFilter += fmt.Sprintf(" AND EXTRACT(EPOCH FROM created_at) >= $%d", len(args))
	}
	if endTime > 0 {
		args = append(args, endTime)
		timeFilter += fmt.Sprintf(" AND EXTRACT(EPOCH FROM created_at) <= $%d", len(args))
	}
	args = append(args, quoteDecimals)
	args = append(args, limit)

	// Aggregate fills into OHLCV buckets using the actual quote asset metadata on each
	// fill row. Pool metadata is not authoritative for chart volume because the canonical
	// quote token may differ from the pair's configured quote symbol or decimals when a
	// pair is inverted or the quote asset is a wrapped asset like WETH on Base.
	fillQuery := `
		SELECT
			EXTRACT(EPOCH FROM created_at)::bigint,
			price::double precision,
			amount_quote,
			COALESCE(NULLIF(quote_token_address, ''), ''),
			COALESCE(NULLIF(quote_token_symbol, ''), '')
		FROM dex_fills
		WHERE pair_id = $1
		  AND settlement_status = 'confirmed'
		  AND price <> '' AND price IS NOT NULL
		  ` + timeFilter + `
		ORDER BY created_at ASC, id ASC`

	rows, err := db.QueryContext(ctx, fillQuery, pairID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bucketMap := make(map[int64]*PairCandle)
	for rows.Next() {
		var createdAtUnix int64
		var fillPrice float64
		var amountQuote, quoteAddress, quoteSymbol string
		if err := rows.Scan(&createdAtUnix, &fillPrice, &amountQuote, &quoteAddress, &quoteSymbol); err != nil {
			return nil, err
		}
		bucketStart := (createdAtUnix / bucketSecs) * bucketSecs * 1000
		bucket, ok := bucketMap[bucketStart]
		if !ok {
			bucket = &PairCandle{Timestamp: bucketStart, Open: fillPrice, High: fillPrice, Low: fillPrice, Close: fillPrice, Volume: 0}
			bucketMap[bucketStart] = bucket
		}
		if fillPrice > bucket.High {
			bucket.High = fillPrice
		}
		if fillPrice < bucket.Low || bucket.Low == 0 {
			bucket.Low = fillPrice
		}
		bucket.Close = fillPrice
		bucket.Volume += normalizeFillAmountQuote(amountQuote, quoteAddress, quoteSymbol)
		if bucket.Open == 0 {
			bucket.Open = fillPrice
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	candles := make([]PairCandle, 0, len(bucketMap))
	for _, candle := range bucketMap {
		// DEX fill prices are stored in display-oriented form (e.g. 0.00040085 for
		// WETH/USDC after shouldFlipPair). Pool candles store the raw price (2494).
		// The frontend's DexKLineChart calls orientCandle(invert=true) on everything,
		// which inverts: pool 2494→0.00040085 ✓, but DEX 0.00040085→2494 ✗.
		// Pre-invert DEX fill candles here so both sources end up on the same
		// raw-pool scale BEFORE the client's orientCandle flips them to display scale.
		if shouldInvert && candle.Open > 0 && candle.Close > 0 {
			rawH, rawL := candle.High, candle.Low
			candle.Open = 1.0 / candle.Open
			candle.Close = 1.0 / candle.Close
			if rawL > 0 {
				candle.High = 1.0 / rawL
			}
			if rawH > 0 {
				candle.Low = 1.0 / rawH
			}
		}
		candle.Turnover = candle.Close * candle.Volume
		candles = append(candles, *candle)
	}
	sort.Slice(candles, func(i, j int) bool { return candles[i].Timestamp < candles[j].Timestamp })
	if limit > 0 && len(candles) > limit {
		candles = candles[len(candles)-limit:]
	}
	return candles, nil
}

// fillInactiveCandleGaps makes the chart timeline continuous without inventing
// price movement. Missing buckets carry forward the previous close and have
// zero volume, matching the convention used by continuous exchange kline feeds.
func fillInactiveCandleGaps(candles []PairCandle, interval string, limit int) []PairCandle {
	bucketSeconds, ok := intervalSeconds[interval]
	if !ok || len(candles) < 2 || limit <= 0 {
		return candles
	}

	bucketMs := bucketSeconds * 1000
	latest := candles[len(candles)-1].Timestamp
	first := latest - int64(limit-1)*bucketMs
	actual := make(map[int64]PairCandle, len(candles))
	for _, candle := range candles {
		if candle.Timestamp >= first && candle.Timestamp <= latest {
			actual[candle.Timestamp] = candle
		}
	}

	filled := make([]PairCandle, 0, limit)
	var previous PairCandle
	for timestamp := first; timestamp <= latest; timestamp += bucketMs {
		if candle, exists := actual[timestamp]; exists {
			previous = candle
			filled = append(filled, candle)
			continue
		}
		if len(filled) == 0 {
			continue
		}
		filled = append(filled, PairCandle{
			Timestamp: timestamp,
			Open:      previous.Close,
			High:      previous.Close,
			Low:       previous.Close,
			Close:     previous.Close,
			Volume:    0,
			Turnover:  0,
		})
	}
	if len(filled) > limit {
		return filled[len(filled)-limit:]
	}
	return filled
}

func pairCandlesHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolAddress := strings.TrimSpace(r.PathValue("id"))
		interval := strings.TrimSpace(r.URL.Query().Get("interval"))
		if poolAddress == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pool_id_required"})
			return
		}
		if _, ok := pairCandleIntervals[interval]; !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_interval"})
			return
		}

		limit := 500
		if requested, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && requested > 0 && requested <= 1000 {
			limit = requested
		}

		var startTime, endTime int64
		if s := r.URL.Query().Get("startTime"); s != "" {
			if v, err := strconv.ParseInt(s, 10, 64); err == nil {
				startTime = v
			}
		}
		if e := r.URL.Query().Get("endTime"); e != "" {
			if v, err := strconv.ParseInt(e, 10, 64); err == nil {
				endTime = v
			}
		}

		query := `SELECT bucket_start, open, high, low, COALESCE(volume, 0), close
			FROM price_candles WHERE pool_address = $1 AND timeframe = $2`
		args := []any{poolAddress, interval}
		if startTime > 0 {
			args = append(args, startTime)
			query += fmt.Sprintf(" AND bucket_start >= $%d", len(args))
		}
		if endTime > 0 {
			args = append(args, endTime)
			query += fmt.Sprintf(" AND bucket_start <= $%d", len(args))
		}
		query += fmt.Sprintf(" ORDER BY bucket_start ASC LIMIT $%d", len(args)+1)
		args = append(args, limit)

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			log.Printf("pair candles query failed: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
			return
		}
		defer rows.Close()

		// Pool candles indexed by timestamp (ms) for fast merging.
		poolByTs := make(map[int64]PairCandle, limit)
		var poolOrder []int64
		for rows.Next() {
			var candle PairCandle
			if err := rows.Scan(&candle.Timestamp, &candle.Open, &candle.High, &candle.Low, &candle.Volume, &candle.Close); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "invalid_candle_data"})
				return
			}
			candle.Turnover = candle.Close * candle.Volume
			poolByTs[candle.Timestamp] = candle
			poolOrder = append(poolOrder, candle.Timestamp)
		}
		if err := rows.Err(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "candle_read_failed"})
			return
		}

		// Fetch DEX fill candles for the same pair/interval.
		dexCandles, dexErr := buildDexCandles(ctx, db, poolAddress, interval, startTime, endTime, limit)
		if dexErr != nil {
			log.Printf("dex candles query failed for %s: %v", poolAddress, dexErr)
			// Non-fatal — fall back to pool-only candles.
			dexCandles = nil
		}

		// Merge: DEX candles are ground truth for buckets where real trades happened.
		// For buckets with only pool data, pool candles provide the background price.
		dexByTs := make(map[int64]PairCandle, len(dexCandles))
		for _, c := range dexCandles {
			dexByTs[c.Timestamp] = c
			if _, exists := poolByTs[c.Timestamp]; !exists {
				poolOrder = append(poolOrder, c.Timestamp)
			}
		}

		// Sort merged bucket list by timestamp.
		sort.Slice(poolOrder, func(i, j int) bool { return poolOrder[i] < poolOrder[j] })
		// Deduplicate into a fresh slice — do NOT reuse poolOrder's backing array.
		seen := make(map[int64]struct{}, len(poolOrder))
		deduped := make([]int64, 0, len(poolOrder))
		for _, ts := range poolOrder {
			if _, ok := seen[ts]; !ok {
				seen[ts] = struct{}{}
				deduped = append(deduped, ts)
			}
		}

		candles := make([]PairCandle, 0, len(deduped))
		for _, ts := range deduped {
			if dex, hasDex := dexByTs[ts]; hasDex {
				// DEX trade data wins — real settled on-chain prices.
				// We intentionally don't add pool volume to avoid non-idempotent
				// floating-point accumulation on every poll cycle (causes shaking).
				candles = append(candles, dex)
			} else if pool, hasPool := poolByTs[ts]; hasPool {
				candles = append(candles, pool)
			}
		}

		// Trim to requested limit.
		if len(candles) > limit {
			candles = candles[len(candles)-limit:]
		}
		candles = fillInactiveCandleGaps(candles, interval, limit)

		writeJSON(w, http.StatusOK, map[string]any{
			"poolAddress":      poolAddress,
			"interval":         interval,
			"priceOrientation": "pool",
			"pricePrecision":   2,
			"candles":          candles,
		})
	}
}

func pairDecimalsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pair_id_required"})
			return
		}
		var base, quote int
		var baseSymbol string
		err := db.QueryRowContext(r.Context(), `
			SELECT base_decimals, quote_decimals, COALESCE(base_symbol,'') FROM pools WHERE address=$1
			UNION ALL SELECT token_mint_0_decimals, token_mint_1_decimals, '' FROM raydium_pools WHERE address=$1
			UNION ALL SELECT token_a_decimals, token_b_decimals, '' FROM meteora_damm_v2_pools WHERE address=$1
			UNION ALL SELECT token_y_decimals, token_x_decimals, COALESCE(token_y_symbol,'') FROM meteora_dlmm_pools WHERE address=$1
			UNION ALL SELECT token_mint_a_decimals, token_mint_b_decimals, '' FROM orca_whirlpools WHERE address=$1
			UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM bsc_pancakeswap_v2_pools WHERE address=$1
			UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM bsc_pancakeswap_v3_pools WHERE address=$1
			UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM bsc_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT currency0_decimals, currency1_decimals, COALESCE(currency0_symbol,'') FROM bsc_uniswap_v4_pools WHERE address=$1
			UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM base_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT currency0_decimals, currency1_decimals, COALESCE(currency0_symbol,'') FROM base_uniswap_v4_pools WHERE address=$1
			UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM robinhood_uniswap_v2_pools WHERE address=$1
			UNION ALL SELECT token0_decimals, token1_decimals, COALESCE(token0_symbol,'') FROM robinhood_uniswap_v3_pools WHERE address=$1
			UNION ALL SELECT currency0_decimals, currency1_decimals, COALESCE(currency0_symbol,'') FROM robinhood_uniswap_v4_pools WHERE address=$1
			LIMIT 1`, id).Scan(&base, &quote, &baseSymbol)
		if err != nil {
			log.Printf("pair decimals lookup failed for %s: %v", id, err)
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "pair_decimals_not_found"})
			return
		}
		if base < 0 || quote < 0 || base > 36 || quote > 36 {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "invalid_pair_decimals"})
			return
		}
		if _, shouldFlip := flipTokens[strings.ToUpper(strings.TrimSpace(baseSymbol))]; shouldFlip {
			base, quote = quote, base
		}
		writeJSON(w, http.StatusOK, map[string]any{"baseDecimals": base, "quoteDecimals": quote, "baseSymbol": baseSymbol})
	}
}

func pairsHandler(db *sql.DB, forcedNetwork *string, cache *pairCache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		network := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("network")))
		if forcedNetwork != nil {
			network = *forcedNetwork
		}
		limit := 200
		if requested, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && requested > 0 && requested <= 1000 {
			limit = requested
		}
		activeOnly := strings.EqualFold(r.URL.Query().Get("active"), "true") || strings.HasSuffix(r.URL.Path, "/active")
		if activeOnly {
			if limit < 50 {
				limit = 50
			}
			if limit > 400 {
				limit = 400
			}
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			pairs, err := cache.get(ctx, db, network, limit)
			if err != nil {
				log.Printf("hot pair cache query failed: %v", err)
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
				return
			}
			writeJSON(w, http.StatusOK, pairs)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		pairs, err := queryPairs(ctx, db, network, limit)
		if err != nil {
			log.Printf("pairs query failed: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, pairs)
	}
}

func queryPairs(ctx context.Context, db *sql.DB, network string, limit int) ([]Pair, error) {
	rows, err := db.QueryContext(ctx, pairQuery, dbNetworkFilter(network), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pairs := make([]Pair, 0)
	for rows.Next() {
		pair, err := scanPairRow(rows)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, pair)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := enrichPairMetrics(ctx, db, pairs); err != nil {
		log.Printf("pair metrics enrichment failed: %v", err)
	}
	return pairs, nil
}

func pairTransactionCounts(ctx context.Context, db *sql.DB, pairID string) (float64, float64, error) {
	var tx1h, tx24h sql.NullFloat64
	err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE((SELECT COUNT(*)::double precision FROM dex_fills WHERE pair_id=$1 AND settlement_status='confirmed' AND created_at >= NOW() - INTERVAL '1 hour'), 0.0),
			COALESCE((SELECT COUNT(*)::double precision FROM dex_fills WHERE pair_id=$1 AND settlement_status='confirmed' AND created_at >= NOW() - INTERVAL '24 hours'), 0.0)
	`, pairID).Scan(&tx1h, &tx24h)
	if err != nil {
		return 0, 0, err
	}
	return tx1h.Float64, tx24h.Float64, nil
}

func enrichPairMetrics(ctx context.Context, db *sql.DB, pairs []Pair) error {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT pair_id FROM dex_orders WHERE network IN ('bsc','base','robinhood','solana') UNION SELECT DISTINCT pair_id FROM dex_fills`)
	if err != nil {
		return err
	}
	metricPairs := make(map[string]struct{})
	for rows.Next() {
		var pairID string
		if err := rows.Scan(&pairID); err != nil {
			rows.Close()
			return err
		}
		metricPairs[pairID] = struct{}{}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for index := range pairs {
		pair := &pairs[index]
		if _, ok := metricPairs[pair.ID]; !ok {
			continue
		}
		var lastPrice, previousPrice, volume24h, bidDepth, askDepth sql.NullFloat64
		tx1h, tx24h, txErr := pairTransactionCounts(ctx, db, pair.ID)
		if txErr == nil {
			pair.TransactionCount1h = &tx1h
			pair.TransactionCount24h = &tx24h
		}
		err := db.QueryRowContext(ctx, `
			WITH decimals AS (
				SELECT COALESCE(
					(SELECT quote_decimals FROM pools WHERE address=$1),
					(SELECT token_mint_1_decimals FROM raydium_pools WHERE address=$1),
					(SELECT token_b_decimals FROM meteora_damm_v2_pools WHERE address=$1),
					(SELECT token_x_decimals FROM meteora_dlmm_pools WHERE address=$1),
					(SELECT token_mint_b_decimals FROM orca_whirlpools WHERE address=$1),
					(SELECT token1_decimals FROM bsc_pancakeswap_v2_pools WHERE address=$1),
					(SELECT token1_decimals FROM bsc_pancakeswap_v3_pools WHERE address=$1),
					(SELECT token1_decimals FROM bsc_uniswap_v3_pools WHERE address=$1),
					(SELECT currency1_decimals FROM bsc_uniswap_v4_pools WHERE address=$1),
					(SELECT token1_decimals FROM base_uniswap_v3_pools WHERE address=$1),
					(SELECT currency1_decimals FROM base_uniswap_v4_pools WHERE address=$1),
					(SELECT token1_decimals FROM robinhood_uniswap_v2_pools WHERE address=$1),
					(SELECT token1_decimals FROM robinhood_uniswap_v3_pools WHERE address=$1),
					(SELECT currency1_decimals FROM robinhood_uniswap_v4_pools WHERE address=$1), 
					CASE WHEN $2 = 'solana' THEN 9 ELSE 18 END) AS quote_decimals,
				COALESCE(
					(SELECT base_decimals FROM pools WHERE address=$1),
					(SELECT token_mint_0_decimals FROM raydium_pools WHERE address=$1),
					(SELECT token_a_decimals FROM meteora_damm_v2_pools WHERE address=$1),
					(SELECT token_y_decimals FROM meteora_dlmm_pools WHERE address=$1),
					(SELECT token_mint_a_decimals FROM orca_whirlpools WHERE address=$1),
					(SELECT token0_decimals FROM bsc_pancakeswap_v2_pools WHERE address=$1),
					(SELECT token0_decimals FROM bsc_pancakeswap_v3_pools WHERE address=$1),
					(SELECT token0_decimals FROM bsc_uniswap_v3_pools WHERE address=$1),
					(SELECT currency0_decimals FROM bsc_uniswap_v4_pools WHERE address=$1),
					(SELECT token0_decimals FROM base_uniswap_v3_pools WHERE address=$1),
					(SELECT currency0_decimals FROM base_uniswap_v4_pools WHERE address=$1),
					(SELECT token0_decimals FROM robinhood_uniswap_v2_pools WHERE address=$1),
					(SELECT token0_decimals FROM robinhood_uniswap_v3_pools WHERE address=$1),
					(SELECT currency0_decimals FROM robinhood_uniswap_v4_pools WHERE address=$1), 
					CASE WHEN $2 = 'solana' THEN 6 ELSE 18 END) AS base_decimals,
				COALESCE((SELECT base_symbol FROM pools WHERE address=$1),
					(SELECT token0_symbol FROM bsc_pancakeswap_v2_pools WHERE address=$1),
					(SELECT token0_symbol FROM bsc_pancakeswap_v3_pools WHERE address=$1),
					(SELECT token0_symbol FROM bsc_uniswap_v3_pools WHERE address=$1),
					(SELECT currency0_symbol FROM bsc_uniswap_v4_pools WHERE address=$1),
					(SELECT token0_symbol FROM base_uniswap_v3_pools WHERE address=$1),
					(SELECT currency0_symbol FROM base_uniswap_v4_pools WHERE address=$1),
					(SELECT token0_symbol FROM robinhood_uniswap_v2_pools WHERE address=$1),
					(SELECT token0_symbol FROM robinhood_uniswap_v3_pools WHERE address=$1),
					(SELECT currency0_symbol FROM robinhood_uniswap_v4_pools WHERE address=$1), '') AS base_symbol
			), fills AS (
				SELECT NULLIF(price, '')::double precision AS price, created_at,
					ROW_NUMBER() OVER (ORDER BY created_at DESC) AS row_number
				FROM dex_fills WHERE pair_id=$1 AND settlement_status='confirmed'
			), depth AS (
				SELECT side,
					COALESCE(SUM(GREATEST(COALESCE(NULLIF(amount, ''), '0')::numeric - COALESCE(NULLIF(filled_amount, ''), '0')::numeric, 0) /
						POWER(10, CASE WHEN UPPER(decimals.base_symbol) IN ('SOL','WETH','WBNB','USDT','USDC')
							THEN decimals.quote_decimals ELSE decimals.base_decimals END) * COALESCE(NULLIF(price, ''), '0')::numeric), 0)::double precision AS notional
				FROM dex_orders, decimals
				WHERE pair_id=$1 AND network=$2 AND status IN ('pending','open','partial') AND expiration > EXTRACT(EPOCH FROM NOW())
				GROUP BY side
			)
			SELECT
				(SELECT price FROM fills WHERE row_number=1),
				(SELECT price FROM fills WHERE row_number=2),
				COALESCE((SELECT SUM(COALESCE(NULLIF(f.amount_quote, ''), '0')::numeric / POWER(10, d.quote_decimals))
					FROM dex_fills f CROSS JOIN decimals d
					WHERE f.pair_id=$1 AND f.settlement_status='confirmed'
					AND f.created_at >= NOW() - INTERVAL '24 hours'), 0)::double precision,
				COALESCE((SELECT notional FROM depth WHERE side='buy'), 0),
					COALESCE((SELECT notional FROM depth WHERE side='sell'), 0)`, pair.ID, pair.Network).Scan(&lastPrice, &previousPrice, &volume24h, &bidDepth, &askDepth)
		if err != nil {
			return err
		}
		if lastPrice.Valid {
			// dex_fills.price is stored in display-oriented terms (already inverted for
			// flip-token base pairs like WETH, WBNB, USDC etc). pair.MarketPrice must be
			// in the same orientation as pair.Price (raw pool price) so the frontend's
			// orientPairPrice call in MarketBar maps it correctly to display value.
			// Re-invert here so MarketBar receives the raw-pool-scale price.
			baseSymUpper := ""
			if pair.BaseSymbol != nil {
				baseSymUpper = strings.ToUpper(strings.TrimSpace(*pair.BaseSymbol))
			}
			isFlip := false
			if _, ok := flipTokens[baseSymUpper]; ok && baseSymUpper != "" {
				isFlip = true
			}
			lp := lastPrice.Float64
			if previousPrice.Valid && previousPrice.Float64 != 0 {
				change := (lastPrice.Float64 - previousPrice.Float64) / previousPrice.Float64 * 100
				pair.MarketChange = &change
			}
			if isFlip && lp > 0 {
				lp = 1.0 / lp
			}
			pair.MarketPrice = &lp
		}
		if volume24h.Valid {
			actualVolume, actualErr := sumConfirmedFillVolume24h(ctx, db, pair.ID)
			if actualErr == nil && actualVolume > 0 {
				pair.Volume24h = &actualVolume
				volume24h.Float64 = actualVolume
			} else {
				pair.Volume24h = &volume24h.Float64
			}
			var volUSD float64
			calculated := false

			var quoteSymbol, quoteAddress sql.NullString
			_ = db.QueryRowContext(ctx, `SELECT quote_token_symbol, quote_token_address FROM dex_fills WHERE pair_id=$1 AND settlement_status='confirmed' ORDER BY created_at DESC LIMIT 1`, pair.ID).Scan(&quoteSymbol, &quoteAddress)
			if !quoteAddress.Valid || strings.TrimSpace(quoteAddress.String) == "" {
				quoteAddress = sql.NullString{}
			}
			if !quoteSymbol.Valid || strings.TrimSpace(quoteSymbol.String) == "" {
				quoteSymbol = sql.NullString{}
			}
			lookupCandidates := quoteVolumeLookupCandidates(quoteAddress.String, quoteSymbol.String)
			for _, candidate := range lookupCandidates {
				if candidate == "" {
					continue
				}
				quotePrice := getTokenPrice(candidate)
				if quotePrice <= 0 {
					continue
				}
				volUSD = volume24h.Float64 * quotePrice
				calculated = true
				method := "coingecko-address"
				if candidate == quoteSymbol.String {
					method = "coingecko-symbol"
				}
				log.Printf("[volume-usd] pair=%s method=%s volume=%.8f quote=%s quoteUSD=%.2f volUSD=%.8f",
					pair.ID, method, volume24h.Float64, candidate, quotePrice, volUSD)
				break
			}

			// The fill quote asset is the canonical source. If the fill quote address is a valid
			// WETH address on Base/Ethereum, we must resolve it through the WETH CoinGecko price cache.
			// Skipping conversion only occurs when no valid quote price is known; we do not force USDC.
			if calculated {
				pair.Volume24hUSD = &volUSD
			} else {
				pair.Volume24hUSD = nil
				log.Printf("[volume-usd] pair=%s SKIPPED USD conversion; no valid quote asset price from fills for pair volume %.8f quoteTokenAddress=%q quoteTokenSymbol=%q",
					pair.ID, volume24h.Float64, quoteAddress.String, quoteSymbol.String)
			}
		}
		// Liquidity = total notional depth across both sides of the book.
		// bid_notional = sum(qty_remaining * price) for all open buy orders
		// ask_notional = sum(qty_remaining * price) for all open sell orders
		// Using bid + ask (not 2×min) so liquidity is non-zero even when
		// only one side has resting orders — common in early-stage markets.
		liquidity := 0.0
		if bidDepth.Valid {
			liquidity += bidDepth.Float64
		}
		if askDepth.Valid {
			liquidity += askDepth.Float64
		}
		pair.Liquidity = &liquidity
		// Liquidity USD = liquidity (quote token notional) × quoteToken USD price.
		// quotePriceUsd: for flipped pairs (WETH base) = tokenPriceUSD (the base token USD price).
		// For non-flipped pairs = tokenPriceUSD / price (converts base-priced USD to quote).
		if liquidity > 0 {
			baseSymUpper := ""
			if pair.BaseSymbol != nil {
				baseSymUpper = strings.ToUpper(strings.TrimSpace(*pair.BaseSymbol))
			}
			_, isFlip := flipTokens[baseSymUpper]
			var quotePriceUSD float64
			if pair.TokenPriceUSD != nil {
				if isFlip && baseSymUpper != "" {
					// flipped: quote = original base symbol (e.g. USDC) → price ≈ $1
					// tokenPriceUSD is price of the displayed base (e.g. USDC) ≈ $1
					quotePriceUSD = *pair.TokenPriceUSD
				} else if pair.Price != nil && *pair.Price > 0 {
					// non-flipped: tokenPriceUSD is in terms of base token
					// quote USD price = tokenPriceUSD / rawPoolPrice
					quotePriceUSD = *pair.TokenPriceUSD / *pair.Price
				}
			}
			if quotePriceUSD > 0 {
				liqUSD := liquidity * quotePriceUSD
				pair.LiquidityUSD = &liqUSD
			}
		}
	}
	return nil
}

func scanPairRow(rows *sql.Rows) (Pair, error) {
	var pair Pair
	var baseSymbol, quoteSymbol sql.NullString
	var price, inversePrice, tokenPriceUSD, priceChange, high24h, low24h sql.NullFloat64
	var liquidity, volume24h sql.NullFloat64
	var updatedAt sql.NullTime
	var baseLogoURL, quoteLogoURL sql.NullString
	if err := rows.Scan(&pair.ID, &pair.PoolType, &pair.Network, &baseSymbol, &quoteSymbol, &pair.BaseToken, &pair.QuoteToken, &baseLogoURL, &quoteLogoURL, &price, &inversePrice, &tokenPriceUSD, &priceChange, &high24h, &low24h, &liquidity, &volume24h, &updatedAt); err != nil {
		return Pair{}, err
	}
	if baseSymbol.Valid {
		pair.BaseSymbol = &baseSymbol.String
	}
	if quoteSymbol.Valid {
		pair.QuoteSymbol = &quoteSymbol.String
	}
	if baseLogoURL.Valid {
		pair.BaseLogoURL = &baseLogoURL.String
	} else {
		value := logoURL(pair.Network, pair.BaseToken)
		if value != "" {
			pair.BaseLogoURL = &value
		}
	}
	if quoteLogoURL.Valid {
		pair.QuoteLogoURL = &quoteLogoURL.String
	} else {
		value := logoURL(pair.Network, pair.QuoteToken)
		if value != "" {
			pair.QuoteLogoURL = &value
		}
	}
	if price.Valid {
		pair.Price = &price.Float64
	}
	if inversePrice.Valid {
		pair.InversePrice = &inversePrice.Float64
	}
	if tokenPriceUSD.Valid {
		pair.TokenPriceUSD = &tokenPriceUSD.Float64
	}
	if priceChange.Valid {
		pair.PriceChange = &priceChange.Float64
	}
	if high24h.Valid {
		pair.High24h = &high24h.Float64
	}
	if low24h.Valid {
		pair.Low24h = &low24h.Float64
	}
	if liquidity.Valid {
		pair.Liquidity = &liquidity.Float64
	}
	if volume24h.Valid {
		pair.Volume24h = &volume24h.Float64
	}
	if updatedAt.Valid {
		value := updatedAt.Time.UTC().Format(time.RFC3339)
		pair.UpdatedAt = &value
	}
	pair.Symbol = symbolFor(pair)
	return pair, nil
}

func searchPairsHandler(db *sql.DB, cache *pairCache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
		network := normalizeNetwork(r.URL.Query().Get("network"))
		limit := 20
		if requested, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && requested > 0 && requested <= 100 {
			limit = requested
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		results := make([]Pair, 0, limit)
		if query != "" {
			hot, err := cache.get(ctx, db, network, 400)
			if err == nil {
				for _, pair := range hot {
					if len(results) >= limit {
						break
					}
					if matchesQuery(pair, query) {
						results = append(results, pair)
					}
				}
			}
			if len(results) < limit {
				fallback, err := querySearchPairs(ctx, db, network, query, limit-len(results))
				if err == nil {
					seen := make(map[string]struct{}, len(results))
					for _, pair := range results {
						seen[pair.ID] = struct{}{}
					}
					for _, pair := range fallback {
						if _, ok := seen[pair.ID]; ok {
							continue
						}
						if len(results) >= limit {
							break
						}
						results = append(results, pair)
						seen[pair.ID] = struct{}{}
					}
				}
			}
		} else {
			pairs, err := cache.get(ctx, db, network, limit)
			if err != nil {
				log.Printf("search hot-pair fallback failed: %v", err)
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
				return
			}
			results = append(results, pairs...)
		}
		writeJSON(w, http.StatusOK, results)
	}
}

func matchesQuery(pair Pair, query string) bool {
	if query == "" {
		return true
	}
	base := ""
	if pair.BaseSymbol != nil {
		base = *pair.BaseSymbol
	}
	quote := ""
	if pair.QuoteSymbol != nil {
		quote = *pair.QuoteSymbol
	}
	candidate := strings.ToLower(strings.Join([]string{pair.Symbol, base, quote, pair.BaseToken, pair.QuoteToken}, " "))
	return strings.Contains(candidate, query)
}

func querySearchPairs(ctx context.Context, db *sql.DB, network string, query string, limit int) ([]Pair, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT address, pool_type, network, base_symbol, quote_symbol, base_mint, quote_mint,
			base_logo_url, quote_logo_url, price, inverse_price, token_price_usd, price_change_percent, high_24h, low_24h,
			liquidity, volume_24h, updated_at
		FROM (
			SELECT p.address, p.pool_type, p.network, p.base_symbol, p.quote_symbol, p.base_mint, p.quote_mint,
				p.base_logo_url, p.quote_logo_url, lp.price, lp.inverse_price, lp.token_price_usd, lp.price_change_percent, lp.high_24h, lp.low_24h,
				CAST(NULL AS DOUBLE PRECISION) as liquidity, CAST(NULL AS DOUBLE PRECISION) as volume_24h, lp.updated_at
			FROM pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
			UNION ALL
			SELECT p.address, p.pool_type, p.network, p.token_mint_0_symbol, p.token_mint_1_symbol, p.token_mint_0, p.token_mint_1,
				p.token_mint_0_logo_url, p.token_mint_1_logo_url, lp.price, lp.inverse_price, lp.token_price_usd, lp.price_change_percent, lp.high_24h, lp.low_24h,
				CAST(NULL AS DOUBLE PRECISION) as liquidity, CAST(NULL AS DOUBLE PRECISION) as volume_24h, lp.updated_at
			FROM raydium_pools p LEFT JOIN latest_prices lp ON lp.pool_address = p.address
		) pairs
		WHERE ($1 = '' OR network = $1)
		AND (
			COALESCE(base_symbol, base_mint) ILIKE '%' || $2 || '%'
			OR COALESCE(quote_symbol, quote_mint) ILIKE '%' || $2 || '%'
			OR base_mint ILIKE '%' || $2 || '%'
			OR quote_mint ILIKE '%' || $2 || '%'
		)
		ORDER BY COALESCE(volume_24h, 0) DESC NULLS LAST, COALESCE(liquidity, 0) DESC NULLS LAST, updated_at DESC NULLS LAST
		LIMIT $3`, dbNetworkFilter(network), query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	pairs := make([]Pair, 0, limit)
	for rows.Next() {
		pair, err := scanPairRow(rows)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, pair)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_ = enrichPairMetrics(ctx, db, pairs)
	return pairs, nil
}

func stringPtr(value string) *string { return &value }

func logoURL(network, token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	switch strings.ToLower(network) {
	case "solana":
		return "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/" + token + "/logo.png"
	case "bsc":
		return "https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/smartchain/assets/" + token + "/logo.png"
	case "base":
		return "https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/base/assets/" + token + "/logo.png"
	default:
		return ""
	}
}

func symbolFor(pair Pair) string {
	base := pair.BaseToken
	quote := pair.QuoteToken
	if pair.BaseSymbol != nil && *pair.BaseSymbol != "" {
		base = *pair.BaseSymbol
	}
	if pair.QuoteSymbol != nil && *pair.QuoteSymbol != "" {
		quote = *pair.QuoteSymbol
	}
	return strings.ToUpper(base + quote)
}

func wsPairHandler(hub *pairHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[ws] Connection attempt from %s, method=%s, upgrade=%s", r.RemoteAddr, r.Method, r.Header.Get("Upgrade"))
		upgrader := websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				log.Printf("[ws] CheckOrigin called for origin: %s", r.Header.Get("Origin"))
				return true
			},
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[ws] Upgrade failed: %v", err)
			return
		}
		log.Printf("[ws] Connection established from %s", r.RemoteAddr)
		defer conn.Close()
		hub.add(conn)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				hub.remove(conn)
				return
			}
		}
	}
}

var pairEventTables = []string{
	"pools",
	"raydium_pools",
	"meteora_damm_v2_pools",
	"meteora_dlmm_pools",
	"orca_whirlpools",
	"bsc_pancakeswap_v2_pools",
	"bsc_pancakeswap_v3_pools",
	"bsc_uniswap_v3_pools",
	"bsc_uniswap_v4_pools",
	"base_uniswap_v4_pools",
	"base_uniswap_v3_pools",
	"aerodrome_slipstream_pools",
	"bsc_pancakeswap_infinity_cl_pools",
	"robinhood_uniswap_v2_pools",
	"robinhood_uniswap_v3_pools",
	"robinhood_uniswap_v4_pools",
	"latest_prices",
}

func ensurePairEventTriggers(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE OR REPLACE FUNCTION notify_pair_event()
		RETURNS trigger AS $$
		BEGIN
			PERFORM pg_notify('pair_events', json_build_object(
				'table_name', TG_TABLE_NAME,
				'action', TG_OP,
				'network', COALESCE(
					(row_to_json(NEW) ->> 'network'),
					(row_to_json(OLD) ->> 'network'),
					'all'
				)
			)::text);
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
	`)
	if err != nil {
		return err
	}
	for _, tableName := range pairEventTables {
		if _, err := db.Exec(fmt.Sprintf(`DROP TRIGGER IF EXISTS pair_event_trigger_%s ON %s;`, tableName, tableName)); err != nil {
			continue
		}
		if _, err := db.Exec(fmt.Sprintf(`
			CREATE TRIGGER pair_event_trigger_%s
			AFTER INSERT OR UPDATE OF updated_at, price, liquidity, volume_24h OR DELETE
			ON %s
			FOR EACH ROW EXECUTE FUNCTION notify_pair_event();
		`, tableName, tableName)); err != nil {
			continue
		}
	}
	return nil
}

func listenForPairEvents(db *sql.DB, cache *pairCache, hub *pairHub) {
	listener := pq.NewListener(databaseURLFromDB(db), 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("pair event listener error: %v", err)
		}
	})
	if err := listener.Listen("pair_events"); err != nil {
		log.Printf("pair event listener failed: %v", err)
		return
	}
	defer listener.Close()
	for {
		select {
		case notification := <-listener.Notify:
			if notification == nil {
				continue
			}
			log.Printf("pair event: %s", notification.Extra)
			for _, network := range []string{"all", "bsc", "base", "solana", "robinhood"} {
				pairs, err := cache.refresh(context.Background(), db, network, 400)
				if err != nil {
					log.Printf("pair event refresh failed for %s: %v", network, err)
					continue
				}
				hub.broadcast(map[string]any{
					"type":    "snapshot",
					"network": network,
					"pairs":   pairs,
				})
			}
		case <-time.After(30 * time.Second):
			if err := listener.Ping(); err != nil {
				log.Printf("pair event listener ping failed: %v", err)
				return
			}
		}
	}
}

func databaseURLFromDB(db *sql.DB) string {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgresql://postgres:postgres@127.0.0.1:54322/postgres?sslmode=disable"
	}
	if !strings.Contains(databaseURL, "sslmode=") {
		if strings.Contains(databaseURL, "?") {
			databaseURL += "&sslmode=disable"
		} else {
			databaseURL += "?sslmode=disable"
		}
	}
	return databaseURL
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// solanaRPCProxyHandler proxies JSON-RPC requests to the Solana RPC endpoint.
// This is needed because browser clients can't call Solana RPC directly due to
// CORS restrictions and authentication requirements on public/QuickNode endpoints.
// The backend runs server-side with no CORS issues.
func solanaRPCProxyHandler() http.HandlerFunc {
	solanaRPC := os.Getenv("SOLANA_RPC_URL")
	if solanaRPC == "" {
		solanaRPC = "https://api.mainnet-beta.solana.com"
	}
	log.Printf("[solana-proxy] Configured RPC: %s", solanaRPC)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method_not_allowed"})
			return
		}
		// Read the JSON-RPC body from the client
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB limit
		if err != nil {
			log.Printf("[solana-proxy] read failed: %v", err)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read_failed"})
			return
		}
		log.Printf("[solana-proxy] Forwarding request to %s: %s", solanaRPC, string(body))
		// Forward to Solana RPC
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, solanaRPC, bytes.NewReader(body))
		if err != nil {
			log.Printf("[solana-proxy] request creation failed: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "proxy_failed"})
			return
		}
		req.Header.Set("Content-Type", "application/json")
		// Forward QuickNode API key if configured
		if apiKey := os.Getenv("SOLANA_RPC_API_KEY"); apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("[solana-proxy] RPC call failed: %v", err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "solana_rpc_unreachable"})
			return
		}
		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("[solana-proxy] response read failed: %v", err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "solana_rpc_read_failed"})
			return
		}
		log.Printf("[solana-proxy] Response (status=%d): %s", resp.StatusCode, string(respBody))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil && !errors.Is(err, http.ErrAbortHandler) {
		log.Printf("response write failed: %v", err)
	}
}
