package main

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/Anurag-AV/financial-streaming-platform/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ProcessorServer processes market data and calculates indicators
type ProcessorServer struct {
	pb.UnimplementedProcessorServiceServer

	// Connection to upstream market data generator
	marketDataClient pb.StockServiceClient
	marketDataConn   *grpc.ClientConn

	// Indicator calculators per symbol
	calculators map[string]*IndicatorCalculator
	calcMu      sync.RWMutex

	// Statistics
	stats   map[string]*ProcessorStats
	statsMu sync.RWMutex
}

// ProcessorStats tracks processing metrics
type ProcessorStats struct {
	Symbol              string
	TicksProcessed      int64
	AnomaliesDetected   int64
	AvgProcessingTimeMs float64
	processingTimes     *RingBuffer
}

// NewProcessorServer creates a new processor server
func NewProcessorServer(marketDataAddr string) (*ProcessorServer, error) {
	// Connect to market data generator
	conn, err := grpc.Dial(marketDataAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := pb.NewStockServiceClient(conn)

	return &ProcessorServer{
		marketDataClient: client,
		marketDataConn:   conn,
		calculators:      make(map[string]*IndicatorCalculator),
		stats:            make(map[string]*ProcessorStats),
	}, nil
}

// Close closes the connection to market data generator
func (ps *ProcessorServer) Close() {
	if ps.marketDataConn != nil {
		ps.marketDataConn.Close()
	}
}

// getOrCreateCalculator gets or creates indicator calculator for symbol
func (ps *ProcessorServer) getOrCreateCalculator(symbol string) *IndicatorCalculator {
	ps.calcMu.Lock()
	defer ps.calcMu.Unlock()

	if calc, exists := ps.calculators[symbol]; exists {
		return calc
	}

	calc := NewIndicatorCalculator(symbol)
	ps.calculators[symbol] = calc

	// Initialize stats
	ps.statsMu.Lock()
	ps.stats[symbol] = &ProcessorStats{
		Symbol:          symbol,
		processingTimes: NewRingBuffer(100),
	}
	ps.statsMu.Unlock()

	return calc
}

// StreamEnrichedData streams processed market data with indicators
func (ps *ProcessorServer) StreamEnrichedData(req *pb.ProcessorStreamRequest, stream pb.ProcessorService_StreamEnrichedDataServer) error {
	log.Printf("📊 New processor stream request: symbols=%v", req.Symbols)

	// Subscribe to market data stream
	marketStream, err := ps.marketDataClient.StreamMarketData(
		context.Background(),
		&pb.StreamRequest{
			Symbols:          req.Symbols,
			UpdateIntervalMs: 500,
		},
	)
	if err != nil {
		log.Printf("❌ Failed to subscribe to market data: %v", err)
		return err
	}

	// Process incoming ticks
	for {
		tick, err := marketStream.Recv()
		if err == io.EOF {
			log.Println("📭 Market data stream ended")
			return nil
		}
		if err != nil {
			log.Printf("❌ Error receiving tick: %v", err)
			return err
		}

		// Process tick
		startTime := time.Now()
		enrichedTick := ps.processTick(tick)
		processingTime := time.Since(startTime)

		// Update stats
		ps.updateStats(tick.Symbol, processingTime, enrichedTick.IsAnomaly)

		// Send enriched tick
		if err := stream.Send(enrichedTick); err != nil {
			log.Printf("❌ Error sending enriched tick: %v", err)
			return err
		}

		log.Printf("📤 Processed %s: VWAP=$%.2f, SMA5=$%.2f, Anomaly=%v (%s)",
			tick.Symbol, enrichedTick.Vwap, enrichedTick.Sma_5Min,
			enrichedTick.IsAnomaly, enrichedTick.AnomalyType)
	}
}

// processTick processes a single tick and calculates indicators
func (ps *ProcessorServer) processTick(tick *pb.StockTick) *pb.EnrichedTick {
	calc := ps.getOrCreateCalculator(tick.Symbol)

	// Update calculator with new data
	calc.Update(tick.Price, tick.Volume)

	// Get all indicators
	vwap, sma5, sma15, ema5, ema15, bUpper, bMiddle, bLower, volatility, momentum := calc.GetIndicators()

	// Detect anomalies
	isAnomaly, anomalyType, anomalyScore := calc.DetectAnomaly(tick.Price)

	return &pb.EnrichedTick{
		Tick:            tick,
		Vwap:            vwap,
		Sma_5Min:        sma5,
		Sma_15Min:       sma15,
		Ema_5Min:        ema5,
		Ema_15Min:       ema15,
		BollingerUpper:  bUpper,
		BollingerMiddle: bMiddle,
		BollingerLower:  bLower,
		Volatility:      volatility,
		PriceMomentum:   momentum,
		IsAnomaly:       isAnomaly,
		AnomalyType:     anomalyType,
		AnomalyScore:    anomalyScore,
	}
}

// updateStats updates processing statistics
func (ps *ProcessorServer) updateStats(symbol string, processingTime time.Duration, isAnomaly bool) {
	ps.statsMu.Lock()
	defer ps.statsMu.Unlock()

	stats, exists := ps.stats[symbol]
	if !exists {
		return
	}

	stats.TicksProcessed++
	if isAnomaly {
		stats.AnomaliesDetected++
	}

	stats.processingTimes.Add(processingTime.Seconds() * 1000) // Convert to ms
	stats.AvgProcessingTimeMs = stats.processingTimes.Mean()
}

// GetStats streams processing statistics
func (ps *ProcessorServer) GetStats(req *pb.ProcessorStreamRequest, stream pb.ProcessorService_GetStatsServer) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			ps.statsMu.RLock()
			for _, symbol := range req.Symbols {
				if stats, exists := ps.stats[symbol]; exists {
					statMsg := &pb.ProcessorStats{
						Symbol:               stats.Symbol,
						TicksProcessed:       stats.TicksProcessed,
						AnomaliesDetected:    stats.AnomaliesDetected,
						AvgProcessingTimeMs:  stats.AvgProcessingTimeMs,
						Timestamp:            time.Now().UnixNano(),
					}

					if err := stream.Send(statMsg); err != nil {
						ps.statsMu.RUnlock()
						return err
					}
				}
			}
			ps.statsMu.RUnlock()
		}
	}
}

func main() {
	log.Println("🔧 Stream Processor Service")
	log.Println("============================")

	// Connect to market data generator
	processor, err := NewProcessorServer("localhost:50051")
	if err != nil {
		log.Fatalf("Failed to connect to market data: %v", err)
	}
	defer processor.Close()

	log.Println("✅ Connected to Market Data Generator")

	// Start gRPC server
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterProcessorServiceServer(grpcServer, processor)

	log.Println("🚀 Processor Service running on :50052")
	log.Println("📊 Calculating VWAP, SMA, EMA, Bollinger Bands...")
	log.Println()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}	