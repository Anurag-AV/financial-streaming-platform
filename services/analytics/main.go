package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	pb "github.com/Anurag-AV/financial-streaming-platform/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// AnalyticsServer provides analytics on market data
type AnalyticsServer struct {
	pb.UnimplementedAnalyticsServiceServer

	// Connection to stream processor
	processorClient pb.ProcessorServiceClient
	processorConn   *grpc.ClientConn

	// Historical data per symbol
	priceHistories map[string]*PriceHistory
	historiesMu    sync.RWMutex

	// Performance tracking
	grpcTracker *PerformanceTracker
	restTracker *PerformanceTracker

	// Latest enriched data
	latestData   map[string]*pb.EnrichedTick
	latestDataMu sync.RWMutex
}

// NewAnalyticsServer creates a new analytics server
func NewAnalyticsServer(processorAddr string) (*AnalyticsServer, error) {
	conn, err := grpc.Dial(processorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := pb.NewProcessorServiceClient(conn)

	return &AnalyticsServer{
		processorClient: client,
		processorConn:   conn,
		priceHistories:  make(map[string]*PriceHistory),
		latestData:      make(map[string]*pb.EnrichedTick),
		grpcTracker:     NewPerformanceTracker(),
		restTracker:     NewPerformanceTracker(),
	}, nil
}

// Close closes connections
func (as *AnalyticsServer) Close() {
	if as.processorConn != nil {
		as.processorConn.Close()
	}
}

// StartDataIngestion starts receiving data from processor
func (as *AnalyticsServer) StartDataIngestion(symbols []string) error {
	stream, err := as.processorClient.StreamEnrichedData(
		context.Background(),
		&pb.ProcessorStreamRequest{
			Symbols:         symbols,
			IncludeRawTicks: false,
		},
	)
	if err != nil {
		return err
	}

	// Goroutine to continuously ingest data
	go func() {
		for {
			enriched, err := stream.Recv()
			if err == io.EOF {
				log.Println("Processor stream ended")
				return
			}
			if err != nil {
				log.Printf("Error receiving from processor: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			// Update histories
			symbol := enriched.Tick.Symbol
			as.historiesMu.Lock()
			if _, exists := as.priceHistories[symbol]; !exists {
				as.priceHistories[symbol] = NewPriceHistory()
			}
			as.priceHistories[symbol].AddPrice(enriched.Tick.Price, enriched.Tick.Timestamp)
			as.historiesMu.Unlock()

			// Update latest data
			as.latestDataMu.Lock()
			as.latestData[symbol] = enriched
			as.latestDataMu.Unlock()
		}
	}()

	return nil
}

// GetPortfolioAnalytics calculates portfolio-level analytics
func (as *AnalyticsServer) GetPortfolioAnalytics(ctx context.Context, req *pb.AnalyticsRequest) (*pb.PortfolioAnalytics, error) {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Milliseconds()
		as.grpcTracker.RecordRequest(float64(latency), 0) // Payload size calculated separately
	}()

	as.historiesMu.RLock()
	defer as.historiesMu.RUnlock()

	if len(req.Symbols) == 0 {
		return &pb.PortfolioAnalytics{}, nil
	}

	// Calculate aggregate metrics
	totalVolatility := 0.0
	totalSharpe := 0.0
	count := 0

	var vars95 []float64
	var vars99 []float64

	for _, symbol := range req.Symbols {
		if history, exists := as.priceHistories[symbol]; exists {
			totalVolatility += history.CalculateVolatility()
			totalSharpe += history.CalculateSharpeRatio()
			vars95 = append(vars95, history.CalculateVaR(0.95))
			vars99 = append(vars99, history.CalculateVaR(0.99))
			count++
		}
	}

	if count == 0 {
		return &pb.PortfolioAnalytics{}, nil
	}

	avgVolatility := totalVolatility / float64(count)
	avgSharpe := totalSharpe / float64(count)

	// Portfolio VaR (simplified - use average)
	avgVaR95 := 0.0
	avgVaR99 := 0.0
	for _, v := range vars95 {
		avgVaR95 += v
	}
	for _, v := range vars99 {
		avgVaR99 += v
	}
	if len(vars95) > 0 {
		avgVaR95 /= float64(len(vars95))
		avgVaR99 /= float64(len(vars99))
	}

	return &pb.PortfolioAnalytics{
		Symbols:            req.Symbols,
		TotalValue:         0, // Would calculate from positions
		TotalReturn:        0, // Would calculate from positions
		PortfolioVolatility: avgVolatility,
		SharpeRatio:        avgSharpe,
		ValueAtRisk_95:     avgVaR95,
		ValueAtRisk_99:     avgVaR99,
		Timestamp:          time.Now().UnixNano(),
	}, nil
}

// GetRiskMetrics streams risk metrics for each symbol
func (as *AnalyticsServer) GetRiskMetrics(req *pb.AnalyticsRequest, stream pb.AnalyticsService_GetRiskMetricsServer) error {
	as.historiesMu.RLock()
	defer as.historiesMu.RUnlock()

	for _, symbol := range req.Symbols {
		if history, exists := as.priceHistories[symbol]; exists {
			metrics := &pb.RiskMetrics{
				Symbol:           symbol,
				Volatility:       history.CalculateVolatility(),
				SharpeRatio:      history.CalculateSharpeRatio(),
				Beta:             1.0, // Simplified - would calculate vs market
				ValueAtRisk_95:   history.CalculateVaR(0.95),
				ValueAtRisk_99:   history.CalculateVaR(0.99),
				MaxDrawdown:      history.CalculateMaxDrawdown(),
				ExpectedReturn:   history.CalculateExpectedReturn(),
				Timestamp:        time.Now().UnixNano(),
			}

			if err := stream.Send(metrics); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetCorrelationMatrix calculates correlation matrix
func (as *AnalyticsServer) GetCorrelationMatrix(ctx context.Context, req *pb.AnalyticsRequest) (*pb.CorrelationMatrix, error) {
	as.historiesMu.RLock()
	defer as.historiesMu.RUnlock()

	var pairs []*pb.CorrelationPair

	// Calculate correlation for each pair
	for i := 0; i < len(req.Symbols); i++ {
		for j := i + 1; j < len(req.Symbols); j++ {
			sym1 := req.Symbols[i]
			sym2 := req.Symbols[j]

			hist1, exists1 := as.priceHistories[sym1]
			hist2, exists2 := as.priceHistories[sym2]

			if exists1 && exists2 {
				corr := CalculateCorrelation(hist1, hist2)
				pairs = append(pairs, &pb.CorrelationPair{
					Symbol1:     sym1,
					Symbol2:     sym2,
					Correlation: corr,
					SampleSize:  int32(len(hist1.GetReturns())),
				})
			}
		}
	}

	return &pb.CorrelationMatrix{
		Symbols:   req.Symbols,
		Pairs:     pairs,
		Timestamp: time.Now().UnixNano(),
	}, nil
}

// GetTradingSignals generates simple trading signals
func (as *AnalyticsServer) GetTradingSignals(req *pb.AnalyticsRequest, stream pb.AnalyticsService_GetTradingSignalsServer) error {
	as.latestDataMu.RLock()
	defer as.latestDataMu.RUnlock()

	for _, symbol := range req.Symbols {
		if data, exists := as.latestData[symbol]; exists {
			// Simple signal logic based on indicators
			signalType := "HOLD"
			reason := "No clear signal"
			confidence := 0.5
			targetPrice := data.Tick.Price

			// Price above EMA = bullish
			if data.Tick.Price > data.Ema_15Min && data.PriceMomentum > 1.0 {
				signalType = "BUY"
				reason = "Price above EMA-15 with positive momentum"
				confidence = 0.7
				targetPrice = data.Tick.Price * 1.02
			} else if data.Tick.Price < data.Ema_15Min && data.PriceMomentum < -1.0 {
				signalType = "SELL"
				reason = "Price below EMA-15 with negative momentum"
				confidence = 0.7
				targetPrice = data.Tick.Price * 0.98
			}

			// Bollinger Band signals
			if data.Tick.Price <= data.BollingerLower {
				signalType = "BUY"
				reason = "Price at lower Bollinger Band - oversold"
				confidence = 0.8
				targetPrice = data.BollingerMiddle
			} else if data.Tick.Price >= data.BollingerUpper {
				signalType = "SELL"
				reason = "Price at upper Bollinger Band - overbought"
				confidence = 0.8
				targetPrice = data.BollingerMiddle
			}

			signal := &pb.TradingSignal{
				Symbol:      symbol,
				SignalType:  signalType,
				Confidence:  confidence,
				Reason:      reason,
				TargetPrice: targetPrice,
				Timestamp:   time.Now().UnixNano(),
			}

			if err := stream.Send(signal); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetPerformanceMetrics returns performance comparison metrics
func (as *AnalyticsServer) GetPerformanceMetrics(ctx context.Context, req *pb.AnalyticsRequest) (*pb.PerformanceMetrics, error) {
	avgLatency, p50, p95, p99, throughput, avgPayload, count := as.grpcTracker.GetMetrics()

	return &pb.PerformanceMetrics{
		ApiType:         "gRPC",
		AvgLatencyMs:    avgLatency,
		P50LatencyMs:    p50,
		P95LatencyMs:    p95,
		P99LatencyMs:    p99,
		RequestCount:    count,
		ThroughputRps:   throughput,
		AvgPayloadBytes: avgPayload,
		Timestamp:       time.Now().UnixNano(),
	}, nil
}

// REST API Handlers

func (as *AnalyticsServer) handleRESTPortfolioAnalytics(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Milliseconds()
		as.restTracker.RecordRequest(float64(latency), 0)
	}()

	symbols := r.URL.Query()["symbol"]
	if len(symbols) == 0 {
		symbols = []string{"AAPL", "GOOGL", "MSFT", "TSLA"}
	}

	analytics, err := as.GetPortfolioAnalytics(context.Background(), &pb.AnalyticsRequest{
		Symbols: symbols,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(analytics)
}

func (as *AnalyticsServer) handleRESTRiskMetrics(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Milliseconds()
		as.restTracker.RecordRequest(float64(latency), 0)
	}()

	symbols := r.URL.Query()["symbol"]
	if len(symbols) == 0 {
		symbols = []string{"AAPL", "GOOGL", "MSFT", "TSLA"}
	}

	as.historiesMu.RLock()
	defer as.historiesMu.RUnlock()

	var metrics []*pb.RiskMetrics
	for _, symbol := range symbols {
		if history, exists := as.priceHistories[symbol]; exists {
			metric := &pb.RiskMetrics{
				Symbol:         symbol,
				Volatility:     history.CalculateVolatility(),
				SharpeRatio:    history.CalculateSharpeRatio(),
				Beta:           1.0,
				ValueAtRisk_95: history.CalculateVaR(0.95),
				ValueAtRisk_99: history.CalculateVaR(0.99),
				MaxDrawdown:    history.CalculateMaxDrawdown(),
				ExpectedReturn: history.CalculateExpectedReturn(),
				Timestamp:      time.Now().UnixNano(),
			}
			metrics = append(metrics, metric)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (as *AnalyticsServer) handleRESTCorrelationMatrix(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Milliseconds()
		as.restTracker.RecordRequest(float64(latency), 0)
	}()

	symbols := r.URL.Query()["symbol"]
	if len(symbols) == 0 {
		symbols = []string{"AAPL", "GOOGL", "MSFT", "TSLA"}
	}

	matrix, err := as.GetCorrelationMatrix(context.Background(), &pb.AnalyticsRequest{
		Symbols: symbols,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(matrix)
}

func (as *AnalyticsServer) handleRESTPerformanceMetrics(w http.ResponseWriter, r *http.Request) {
	avgLatency, p50, p95, p99, throughput, avgPayload, count := as.restTracker.GetMetrics()

	metrics := &pb.PerformanceMetrics{
		ApiType:         "REST",
		AvgLatencyMs:    avgLatency,
		P50LatencyMs:    p50,
		P95LatencyMs:    p95,
		P99LatencyMs:    p99,
		RequestCount:    count,
		ThroughputRps:   throughput,
		AvgPayloadBytes: avgPayload,
		Timestamp:       time.Now().UnixNano(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func main() {
	log.Println("Analytics Engine Service")
	log.Println("=========================")

	// Connect to stream processor
	analytics, err := NewAnalyticsServer("localhost:50052")
	if err != nil {
		log.Fatalf("Failed to connect to processor: %v", err)
	}
	defer analytics.Close()

	log.Println("Connected to Stream Processor")

	// Start ingesting data
	symbols := []string{"AAPL", "GOOGL", "MSFT", "TSLA", "NVDA", "META"}
	if err := analytics.StartDataIngestion(symbols); err != nil {
		log.Fatalf("Failed to start data ingestion: %v", err)
	}

	log.Println("Data ingestion started for:", symbols)

	// Start gRPC server
	lis, err := net.Listen("tcp", ":50053")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAnalyticsServiceServer(grpcServer, analytics)

	go func() {
		log.Println("gRPC Analytics Service running on :50053")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Start REST server
	http.HandleFunc("/api/portfolio", analytics.handleRESTPortfolioAnalytics)
	http.HandleFunc("/api/risk", analytics.handleRESTRiskMetrics)
	http.HandleFunc("/api/correlation", analytics.handleRESTCorrelationMatrix)
	http.HandleFunc("/api/performance", analytics.handleRESTPerformanceMetrics)

	log.Println("REST API running on :8080")
	log.Println("Endpoints:")
	log.Println("  GET /api/portfolio?symbol=AAPL&symbol=GOOGL")
	log.Println("  GET /api/risk?symbol=AAPL&symbol=GOOGL")
	log.Println("  GET /api/correlation?symbol=AAPL&symbol=GOOGL&symbol=MSFT")
	log.Println("  GET /api/performance")
	log.Println()

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to serve REST: %v", err)
	}
}