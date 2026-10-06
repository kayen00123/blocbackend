package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestLoadEnvFileFallsBackFromPlaceholder(t *testing.T) {
	t.Setenv("SETTLEMENT_API_KEY", "")

	dir := t.TempDir()
	localPath := filepath.Join(dir, "backend.env")
	rootPath := filepath.Join(dir, "root.env")
	if err := os.WriteFile(localPath, []byte("SETTLEMENT_API_KEY=replace-with-a-long-random-local-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rootPath, []byte("SETTLEMENT_API_KEY=the-shared-test-key\n"), 0600); err != nil {
		t.Fatal(err)
	}

	loadEnvFile(localPath)
	loadEnvFile(rootPath)
	if got := os.Getenv("SETTLEMENT_API_KEY"); got != "the-shared-test-key" {
		t.Fatalf("SETTLEMENT_API_KEY = %q, want root env value", got)
	}
}

func encodeBase58(value []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	encoded := ""
	for _, byteValue := range value {
		carry := int(byteValue)
		for index := len(encoded) - 1; index >= 0; index-- {
			carry += stringsIndex(alphabet, encoded[index]) * 256
			encoded = encoded[:index] + string(alphabet[carry%58]) + encoded[index+1:]
			carry /= 58
		}
		for carry > 0 {
			encoded = string(alphabet[carry%58]) + encoded
			carry /= 58
		}
	}
	for _, byteValue := range value {
		if byteValue != 0 {
			break
		}
		encoded = "1" + encoded
	}
	return encoded
}

func stringsIndex(alphabet string, value byte) int {
	for index := range alphabet {
		if alphabet[index] == value {
			return index
		}
	}
	return -1
}

func TestVerifySolanaOrderSignature(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	order := CreateOrderRequest{
		Maker: "", TokenIn: "TokenIn", TokenOut: "TokenOut", AmountIn: "100", Amount: "100",
		AmountOutMin: "95", Expiration: 2_000_000_000, Nonce: 7, Salt: 8, PairID: "Pair",
		Side: "sell", Price: "1.25", Network: "solana", TokenInAccount: "Source", TokenOutAccount: "Destination",
	}
	order.Maker = encodeBase58(publicKey)
	body := canonicalSolanaOrderBody(order)
	order.Signature = encodeBase58(ed25519.Sign(privateKey, append([]byte("AltBloc DEX Order:\n"), body...)))
	if err := verifySolanaOrderSignature(order); err != nil {
		t.Fatalf("valid Solana signature rejected: %v", err)
	}
	order.Price = "1.26"
	if err := verifySolanaOrderSignature(order); err == nil {
		t.Fatal("tampered Solana order signature was accepted")
	}
}

func TestVerifySolanaOrderSignatureUsesCanonicalKeyOrdering(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	order := CreateOrderRequest{
		Maker: encodeBase58(publicKey),
		TokenIn: "TokenIn", TokenOut: "TokenOut", AmountIn: "100", Amount: "100",
		AmountOutMin: "95", Expiration: 2_000_000_000, Nonce: 7, Salt: 8, PairID: "Pair",
		Side: "sell", Price: "1.25", Network: "solana", TokenInAccount: "Source", TokenOutAccount: "Destination",
	}
	message := map[string]interface{}{
		"amount":          order.Amount,
		"amountIn":        order.AmountIn,
		"amountOutMin":    order.AmountOutMin,
		"expiration":      order.Expiration,
		"maker":           order.Maker,
		"network":         order.Network,
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
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	order.Signature = encodeBase58(ed25519.Sign(privateKey, append([]byte("AltBloc DEX Order:\n"), body...)))
	if err := verifySolanaOrderSignature(order); err != nil {
		t.Fatalf("canonical key-order Solana signature rejected: %v", err)
	}
}

func TestVerifySolanaOrderSignatureAcceptsLegacyRawBody(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	order := CreateOrderRequest{
		Maker: encodeBase58(publicKey),
		TokenIn: "TokenIn", TokenOut: "TokenOut", AmountIn: "100", Amount: "100",
		AmountOutMin: "95", Expiration: 2_000_000_000, Nonce: 7, Salt: 8, PairID: "Pair",
		Side: "sell", Price: "1.25", Network: "solana", TokenInAccount: "Source", TokenOutAccount: "Destination",
	}
	body := canonicalSolanaOrderBody(order)
	order.Signature = encodeBase58(ed25519.Sign(privateKey, body))
	if err := verifySolanaOrderSignature(order); err != nil {
		t.Fatalf("legacy raw-body Solana signature rejected: %v", err)
	}
}

func TestParsePriceAcceptsTinyPositiveValues(t *testing.T) {
	if _, err := parsePrice("0.000000003"); err != nil {
		t.Fatalf("parsePrice should accept tiny positive prices, got %v", err)
	}
	if _, err := parsePrice("3e-9"); err != nil {
		t.Fatalf("parsePrice should accept scientific-notation tiny positive prices, got %v", err)
	}
}

func TestDBNetworkFilter(t *testing.T) {
	tests := map[string]string{
		"":           "",
		"all":        "",
		"ALL":        "",
		"  solana  ": "solana",
		"base":       "base",
		"robinhood":  "robinhood",
	}

	for input, want := range tests {
		if got := dbNetworkFilter(input); got != want {
			t.Fatalf("dbNetworkFilter(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestResolveTriggeredConditionalOrderUsesBookPriceForMarketExecution(t *testing.T) {
	orderType, fillPrice := resolveTriggeredConditionalOrder("market", "0.00968275", "0.006")
	if orderType != "market" {
		t.Fatalf("market conditional order should remain market, got %q", orderType)
	}
	if fillPrice != "" {
		t.Fatalf("market conditional order should not carry a fixed price; it must match the resting book, got %q", fillPrice)
	}

	orderType, fillPrice = resolveTriggeredConditionalOrder("limit", "0.00968275", "0.006")
	if orderType != "limit" {
		t.Fatalf("limit conditional order should remain limit, got %q", orderType)
	}
	if fillPrice != "0.006" {
		t.Fatalf("limit conditional fill price should use trigger price, got %q want %q", fillPrice, "0.006")
	}
}

func TestSanitizeConditionalOrderRequestClearsPriceForMarketExecution(t *testing.T) {
	req := &CreateOrderRequest{
		OrderType:     "take_profit",
		ExecutionType: "market",
		Price:         "0.006",
		TriggerPrice:  "0.006",
	}
	sanitizeConditionalOrderRequest(req)
	if req.Price != "" {
		t.Fatalf("market conditional order price must be blank, got %q", req.Price)
	}
	if req.TriggerPrice != "0.006" {
		t.Fatalf("market conditional trigger price should remain set, got %q", req.TriggerPrice)
	}
}

func TestSanitizeMarketOrderRequestClearsPriceForTrueMarketOrders(t *testing.T) {
	req := &CreateOrderRequest{OrderType: "market", Price: "0.0096897", AmountOutMin: "123"}
	sanitizeMarketOrderRequest(req)
	if req.Price != "" {
		t.Fatalf("market order price must be blank, got %q", req.Price)
	}
	if req.AmountOutMin != "0" {
		t.Fatalf("market order amountOutMin must be zero, got %q", req.AmountOutMin)
	}
}

func TestResolveMarketReferencePriceUsesLatestAvailablePrice(t *testing.T) {
	ref, ok := resolveMarketReferencePrice("", "0.000000003", "bad")
	if !ok {
		t.Fatal("reference price should resolve from a valid candidate")
	}
	want := new(big.Rat)
	if _, ok := want.SetString("0.000000003"); !ok {
		t.Fatal("could not build expected 3e-9 rational")
	}
	if ref.Cmp(want) != 0 {
		t.Fatalf("reference rate mismatch: got %s want %s", ref.FloatString(12), want.FloatString(12))
	}
}

func TestShouldSkipRestingOrderWhenBuyQuoteWouldExceedRemaining(t *testing.T) {
	if !shouldSkipRestingOrderForBuy(big.NewInt(110), big.NewInt(100)) {
		t.Fatal("quote-exhausted resting order should be skipped so the next valid ladder level can still match")
	}
	if shouldSkipRestingOrderForBuy(big.NewInt(90), big.NewInt(100)) {
		t.Fatal("resting order that still fits within the buyer budget should not be skipped")
	}
}

func TestScoreActivePairWeightsTransactionCountForTrending(t *testing.T) {
	pairWithMoreTrades := Pair{
		Volume24h:           float64Ptr(25),
		TransactionCount1h:  float64Ptr(40),
		TransactionCount24h: float64Ptr(300),
	}
	pairWithMoreVolume := Pair{
		Volume24h:           float64Ptr(500),
		TransactionCount1h:  float64Ptr(0),
		TransactionCount24h: float64Ptr(0),
	}
	if scoreActivePair(pairWithMoreTrades) <= scoreActivePair(pairWithMoreVolume) {
		t.Fatal("pair with more transactions should rank above a pair with only more volume when trending is based on activity")
	}
}

func float64Ptr(value float64) *float64 {
	return &value
}

func TestComputeFillQuoteForMarketOrdersUsesReferencePrice(t *testing.T) {
	price, ok := new(big.Rat).SetString("0.5")
	if !ok {
		t.Fatal("could not parse reference price")
	}
	got := computeFillQuoteFromReferencePrice(big.NewInt(2000), price)
	if got == nil || got.Sign() <= 0 {
		t.Fatal("market-order fill quote must be positive")
	}
	want := big.NewInt(1000)
	if got.Cmp(want) != 0 {
		t.Fatalf("market-order quote mismatch: got %s want %s", got.String(), want.String())
	}
}

func TestComputeFillPriceFromQuoteAmountDerivesPriceForMarketFill(t *testing.T) {
	base := big.NewInt(2000)
	quote := big.NewInt(1000)
	got := computeFillPriceFromQuoteAmount(base, quote, false)
	if got == "0" {
		t.Fatal("market fill price should be derived from amount/base ratio, not zero")
	}
	if got != "0.5" {
		t.Fatalf("market fill price mismatch: got %s want 0.5", got)
	}

	inverted := computeFillPriceFromQuoteAmount(base, quote, true)
	if inverted != "2" {
		t.Fatalf("flipped pair market fill price should use inverse of raw ratio: got %s want 2", inverted)
	}
}

func TestQuoteVolumeLookupCandidatesPreferAddressThenSymbol(t *testing.T) {
	candidates := quoteVolumeLookupCandidates("0x4200000000000000000000000000000000000006", "")
	if len(candidates) != 1 || candidates[0] != "0x4200000000000000000000000000000000000006" {
		t.Fatalf("address-based lookup should be used as the primary quote candidate, got %#v", candidates)
	}

	candidates = quoteVolumeLookupCandidates("", "SOL")
	if len(candidates) != 1 || candidates[0] != "SOL" {
		t.Fatalf("symbol-based lookup should be used when no address exists, got %#v", candidates)
	}

	candidates = quoteVolumeLookupCandidates("0x4200000000000000000000000000000000000006", "SOL")
	if len(candidates) != 2 || candidates[0] != "0x4200000000000000000000000000000000000006" || candidates[1] != "SOL" {
		t.Fatalf("address should win before symbol when both are available, got %#v", candidates)
	}
}

func TestGetTokenPriceAcceptsBaseWETHAddress(t *testing.T) {
	globalTokenPriceCache.mu.Lock()
	globalTokenPriceCache.prices = map[string]float64{"WETH": 2670.0, "weth": 2670.0, "0x4200000000000000000000000000000000000006": 2670.0, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": 2670.0}
	globalTokenPriceCache.lastUpdate = time.Now()
	globalTokenPriceCache.mu.Unlock()

	if price := getTokenPrice("0x4200000000000000000000000000000000000006"); price != 2670.0 {
		t.Fatalf("Base WETH address should resolve to WETH USD price, got %.2f", price)
	}
}

func TestNormalizeFillAmountQuoteUsesActualQuoteTokenDecimals(t *testing.T) {
	if got := normalizeFillAmountQuote("1000000000000000000", "0x4200000000000000000000000000000000000006", ""); got != 1.0 {
		t.Fatalf("Base WETH quote amount should be normalized using 18 decimals, got %.18f", got)
	}
	if got := normalizeFillAmountQuote("1234567", "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", ""); got != 1.234567 {
		t.Fatalf("Base USDC quote amount should be normalized using 6 decimals, got %.6f", got)
	}
}

func TestResolveQuoteFillDecimalsUsesCachedAddressDecimals(t *testing.T) {
	globalTokenDecimalsCache.set("0x1111111111111111111111111111111111111111", 9)
	defer globalTokenDecimalsCache.set("0x1111111111111111111111111111111111111111", 0)

	if got := resolveQuoteFillDecimals("0x1111111111111111111111111111111111111111", ""); got != 9 {
		t.Fatalf("cached token decimals should be reused for the stored quote address, got %d want 9", got)
	}
}

func TestPairHubBroadcastSerializesConcurrentWrites(t *testing.T) {
	hub := newPairHub()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade failed: %v", err)
		}
		defer conn.Close()
		hub.add(conn)
		defer hub.remove(conn)

		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(value int) {
				defer wg.Done()
				hub.broadcast(map[string]int{"value": value})
			}(i)
		}
		wg.Wait()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	for i := 0; i < 20; i++ {
		var payload map[string]int
		if err := conn.ReadJSON(&payload); err != nil {
			t.Fatalf("read message %d failed: %v", i, err)
		}
		if payload["value"] < 0 || payload["value"] >= 20 {
			t.Fatalf("unexpected payload: %+v", payload)
		}
	}
}

func TestPairCandleQueryOrderUsesNewestRowsForUnboundedPolls(t *testing.T) {
	if got := pairCandleQueryOrder(0, 0); got != "DESC" {
		t.Fatalf("unbounded candle polls should query newest rows first, got %q", got)
	}
	if got := pairCandleQueryOrder(1_000, 2_000); got != "ASC" {
		t.Fatalf("bounded history ranges should stay oldest-first, got %q", got)
	}
}

func TestFillInactiveCandleGapsCarriesForwardClose(t *testing.T) {
	candles := fillInactiveCandleGaps([]PairCandle{
		{Timestamp: 60_000, Open: 10, High: 11, Low: 9, Close: 10, Volume: 2},
		{Timestamp: 180_000, Open: 12, High: 13, Low: 11, Close: 12, Volume: 3},
	}, "1m", 3)

	if len(candles) != 3 {
		t.Fatalf("got %d candles, want 3", len(candles))
	}
	if candles[1].Timestamp != 120_000 || candles[1].Open != 10 || candles[1].High != 10 || candles[1].Low != 10 || candles[1].Close != 10 || candles[1].Volume != 0 {
		t.Fatalf("inactive candle was not a flat zero-volume carry-forward: %+v", candles[1])
	}
}

func TestFillCandleTimeFilterUsesUnixMilliseconds(t *testing.T) {
	startTimeMs := int64(1_700_000_123_456)
	endTimeMs := int64(1_700_000_234_567)
	filter, args := fillCandleTimeFilter([]any{"pair", int64(60)}, startTimeMs, endTimeMs)

	if filter != " AND created_at >= $3 AND created_at <= $4" {
		t.Fatalf("unexpected time filter: %q", filter)
	}
	if len(args) != 4 {
		t.Fatalf("got %d query args, want 4", len(args))
	}
	start, ok := args[2].(time.Time)
	if !ok || !start.Equal(time.UnixMilli(startTimeMs)) {
		t.Fatalf("start argument = %#v, want %v", args[2], time.UnixMilli(startTimeMs))
	}
	end, ok := args[3].(time.Time)
	if !ok || !end.Equal(time.UnixMilli(endTimeMs)) {
		t.Fatalf("end argument = %#v, want %v", args[3], time.UnixMilli(endTimeMs))
	}
}
