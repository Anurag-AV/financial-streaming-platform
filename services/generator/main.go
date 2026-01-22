package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "github.com/Anurag-AV/financial-streaming-platform/proto"
	"google.golang.org/grpc"
)

// StockGenerator simulates realistic stock price movements with technical indicators
type StockGenerator struct {
	symbol       string
	basePrice    float64
	currentPrice float64
	volatility   float64
	dayOpen      float64
	dayHigh      float64
	dayLow       float64
	mu           sync.RWMutex
}

// NewStockGenerator creates a new stock price simulator
func NewStockGenerator(symbol string, basePrice, volatility float64) *StockGenerator {
	return &StockGenerator{
		symbol:       symbol,
		basePrice:    basePrice,
		currentPrice: basePrice,
		volatility:   volatility,
		dayOpen:      basePrice,
		dayHigh:      basePrice,
		dayLow:       basePrice,
	}
}

// GenerateTick creates a new stock tick with realistic price movement
func (sg *StockGenerator) GenerateTick() *pb.StockTick {
	sg.mu.Lock()
	defer sg.mu.Unlock()

	// Random walk with mean reversion (Ornstein-Uhlenbeck process)
	drift := (sg.basePrice - sg.currentPrice) * 0.01
	randomShock := rand.NormFloat64() * sg.volatility
	sg.currentPrice += drift + randomShock

	// Price floor at $1
	if sg.currentPrice < 1.0 {
		sg.currentPrice = 1.0
	}

	// Update day high/low
	if sg.currentPrice > sg.dayHigh {
		sg.dayHigh = sg.currentPrice
	}
	if sg.currentPrice < sg.dayLow {
		sg.dayLow = sg.currentPrice
	}

	// Calculate bid-ask spread (0.05% - 0.15% depending on volatility)
	spreadPercent := 0.0005 + (sg.volatility / sg.basePrice * 0.001)
	spread := sg.currentPrice * spreadPercent
	bid := sg.currentPrice - spread/2
	ask := sg.currentPrice + spread/2

	// Change percent from open
	changePercent := ((sg.currentPrice - sg.dayOpen) / sg.dayOpen) * 100

	return &pb.StockTick{
		Symbol:        sg.symbol,
		Price:         sg.currentPrice,
		Volume:        rand.Int63n(100000) + 10000,
		Bid:           bid,
		Ask:           ask,
		Timestamp:     time.Now().UnixNano(),
		TradeType:     []string{"BUY", "SELL"}[rand.Intn(2)],
		MarketDepth:   rand.Int31n(500) + 50,
		DayHigh:       sg.dayHigh,
		DayLow:        sg.dayLow,
		ChangePercent: changePercent,
	}
}

// GetCurrentPrice returns the current price (thread-safe)
func (sg *StockGenerator) GetCurrentPrice() float64 {
	sg.mu.RLock()
	defer sg.mu.RUnlock()
	return sg.currentPrice
}

// PriceAlert represents an active price alert
type PriceAlert struct {
	Symbol      string
	TargetPrice float64
	Condition   string // "ABOVE" or "BELOW"
	Triggered   bool
	mu          sync.Mutex
}

// CheckAndTrigger checks if alert conditions are met
func (pa *PriceAlert) CheckAndTrigger(currentPrice float64) bool {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	if pa.Triggered {
		return false
	}

	shouldTrigger := false
	if pa.Condition == "ABOVE" && currentPrice >= pa.TargetPrice {
		shouldTrigger = true
	} else if pa.Condition == "BELOW" && currentPrice <= pa.TargetPrice {
		shouldTrigger = true
	}

	if shouldTrigger {
		pa.Triggered = true
	}

	return shouldTrigger
}

// server implements the StockService
type server struct {
	pb.UnimplementedStockServiceServer
	generators   map[string]*StockGenerator
	alerts       []*PriceAlert
	alertStreams []pb.StockService_SubscribePriceAlertsServer
	alertsMu     sync.RWMutex
	streamsMu    sync.RWMutex
	mu           sync.RWMutex
}

func newServer() *server {
	s := &server{
		generators: make(map[string]*StockGenerator),
		alerts:     make([]*PriceAlert, 0),
		alertStreams: make([]pb.StockService_SubscribePriceAlertsServer, 0),
	}

	// Initialize realistic stock generators
	stocks := map[string]struct{ price, volatility float64 }{
		"AAPL":  {175.50, 1.2},
		"GOOGL": {142.30, 1.5},
		"MSFT":  {378.90, 1.0},
		"AMZN":  {178.25, 1.8},
		"TSLA":  {242.80, 3.5},
		"NVDA":  {495.20, 2.0},
		"META":  {389.75, 1.7},
		"NFLX":  {485.30, 2.2},
		"AMD":   {165.80, 2.5},
		"INTC":  {43.25, 0.8},
	}

	for symbol, params := range stocks {
		s.generators[symbol] = NewStockGenerator(symbol, params.price, params.volatility)
	}

	return s
}

// GetStockPrice - unary RPC (keep from yesterday)
func (s *server) GetStockPrice(ctx context.Context, req *pb.StockRequest) (*pb.StockTick, error) {
	s.mu.RLock()
	gen, exists := s.generators[req.Symbol]
	s.mu.RUnlock()

	if !exists {
		gen = NewStockGenerator(req.Symbol, 100.0, 1.0)
		s.mu.Lock()
		s.generators[req.Symbol] = gen
		s.mu.Unlock()
	}

	return gen.GenerateTick(), nil
}

// StreamMarketData - streaming RPC
func (s *server) StreamMarketData(req *pb.StreamRequest, stream pb.StockService_StreamMarketDataServer) error {
	log.Printf("📡 New stream: symbols=%v, interval=%dms", req.Symbols, req.UpdateIntervalMs)

	// Default to 1 second if not specified
	interval := time.Duration(req.UpdateIntervalMs) * time.Millisecond
	if interval == 0 {
		interval = 1000 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Stream until client disconnects or context cancelled
	for {
		select {
		case <-stream.Context().Done():
			log.Printf("📴 Stream ended: %v", stream.Context().Err())
			return nil

		case <-ticker.C:
			// Generate and send tick for each symbol
			for _, symbol := range req.Symbols {
				s.mu.RLock()
				gen, exists := s.generators[symbol]
				s.mu.RUnlock()

				if !exists {
					log.Printf("⚠️  Unknown symbol: %s", symbol)
					continue
				}

				tick := gen.GenerateTick()

				// Check if any alerts should trigger
				s.checkAlerts(tick)

				if err := stream.Send(tick); err != nil {
					log.Printf("❌ Send error: %v", err)
					return err
				}

				log.Printf("📤 %s: $%.2f (%.2f%%)", tick.Symbol, tick.Price, tick.ChangePercent)
			}
		}
	}
}

// SubscribePriceAlerts - bidirectional streaming for price alerts
func (s *server) SubscribePriceAlerts(stream pb.StockService_SubscribePriceAlertsServer) error {
	log.Println("🔔 New price alert subscription")

	// Add this stream to active streams
	s.streamsMu.Lock()
	s.alertStreams = append(s.alertStreams, stream)
	streamIndex := len(s.alertStreams) - 1
	s.streamsMu.Unlock()

	// Remove stream when done
	defer func() {
		s.streamsMu.Lock()
		if streamIndex < len(s.alertStreams) {
			s.alertStreams = append(s.alertStreams[:streamIndex], s.alertStreams[streamIndex+1:]...)
		}
		s.streamsMu.Unlock()
		log.Println("🔔 Alert subscription ended")
	}()

	// Start background alert monitor for this client
	ctx := stream.Context()
	alertChan := make(chan *pb.PriceAlert, 10)

	// Goroutine to send alerts to client
	go func() {
		for alert := range alertChan {
			if err := stream.Send(alert); err != nil {
				log.Printf("❌ Error sending alert: %v", err)
				return
			}
		}
	}()

	// Goroutine to check prices periodically
	go s.monitorPricesForAlerts(ctx, alertChan)

	// Receive alert requests from client
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			close(alertChan)
			return nil
		}
		if err != nil {
			log.Printf("❌ Error receiving alert request: %v", err)
			close(alertChan)
			return err
		}

		log.Printf("🔔 Alert registered: %s %s $%.2f", req.Symbol, req.Condition, req.TargetPrice)

		alert := &PriceAlert{
			Symbol:      req.Symbol,
			TargetPrice: req.TargetPrice,
			Condition:   req.Condition,
			Triggered:   false,
		}

		s.alertsMu.Lock()
		s.alerts = append(s.alerts, alert)
		s.alertsMu.Unlock()

		// Send confirmation
		confirmMsg := &pb.PriceAlert{
			Symbol:       req.Symbol,
			CurrentPrice: 0,
			TargetPrice:  req.TargetPrice,
			Condition:    req.Condition,
			Timestamp:    time.Now().UnixNano(),
			Message:      "Alert registered successfully",
		}

		alertChan <- confirmMsg
	}
}

// NEW: Background monitor that checks prices and triggers alerts
func (s *server) monitorPricesForAlerts(ctx context.Context, alertChan chan<- *pb.PriceAlert) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.alertsMu.Lock()
			s.mu.RLock()

			for _, alert := range s.alerts {
				if gen, exists := s.generators[alert.Symbol]; exists {
					currentPrice := gen.GetCurrentPrice()

					if alert.CheckAndTrigger(currentPrice) {
						log.Printf("🚨 ALERT TRIGGERED: %s %s $%.2f (current: $%.2f)",
							alert.Symbol, alert.Condition, alert.TargetPrice, currentPrice)

						alertMsg := &pb.PriceAlert{
							Symbol:       alert.Symbol,
							CurrentPrice: currentPrice,
							TargetPrice:  alert.TargetPrice,
							Condition:    alert.Condition,
							Timestamp:    time.Now().UnixNano(),
							Message:      fmt.Sprintf("🚨 Price crossed $%.2f threshold!", alert.TargetPrice),
						}

						select {
						case alertChan <- alertMsg:
						default:
							// Channel full, skip
						}
					}
				}
			}

			s.mu.RUnlock()
			s.alertsMu.Unlock()
		}
	}
}

// checkAlerts checks if current price triggers any alerts and notifies
func (s *server) checkAlerts(tick *pb.StockTick) []*pb.PriceAlert {
	s.alertsMu.Lock()
	defer s.alertsMu.Unlock()

	triggered := make([]*pb.PriceAlert, 0)

	for _, alert := range s.alerts {
		if alert.Symbol == tick.Symbol && alert.CheckAndTrigger(tick.Price) {
			log.Printf("🚨 ALERT TRIGGERED: %s %s $%.2f (current: $%.2f)",
				alert.Symbol, alert.Condition, alert.TargetPrice, tick.Price)

			// Create alert notification
			alertMsg := &pb.PriceAlert{
				Symbol:       alert.Symbol,
				CurrentPrice: tick.Price,
				TargetPrice:  alert.TargetPrice,
				Condition:    alert.Condition,
				Timestamp:    time.Now().UnixNano(),
				Message:      fmt.Sprintf("Price %s target!", alert.Condition),
			}
			triggered = append(triggered, alertMsg)
		}
	}

	return triggered
}

func main() {
	rand.Seed(time.Now().UnixNano())

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterStockServiceServer(grpcServer, newServer())

	log.Println("🚀 Market Data Generator Service")
	log.Println("================================")
	log.Println("📊 Streaming real-time stock data on :50051")
	log.Println("🔔 Price alerts enabled")
	log.Println()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
