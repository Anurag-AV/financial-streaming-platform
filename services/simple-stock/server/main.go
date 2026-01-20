package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"time"

	pb "github.com/Anurag-AV/financial-streaming-platform/proto"
	"google.golang.org/grpc"
)

// server implements the StockService
type server struct {
	pb.UnimplementedStockServiceServer
}

// GetStockPrice returns a simulated stock price
func (s *server) GetStockPrice(ctx context.Context, req *pb.StockRequest) (*pb.StockTick, error) {
	// Simulate different base prices for different stocks
	basePrices := map[string]float64{
		"AAPL":  175.50,
		"GOOGL": 142.30,
		"MSFT":  378.90,
		"AMZN":  178.25,
		"TSLA":  242.80,
	}
	
	basePrice, exists := basePrices[req.Symbol]
	if !exists {
		basePrice = 100.0 // Default for unknown symbols
	}

	// Add random variation (+/- 2%)
	variation := (rand.Float64()*0.04 - 0.02) * basePrice
	currentPrice := basePrice + variation

	// Calculate bid-ask spread (0.1% of price)
	spread := currentPrice * 0.001
	bid := currentPrice - spread/2
	ask := currentPrice + spread/2

	// Create response
	tick := &pb.StockTick{
		Symbol:      req.Symbol,
		Price:       currentPrice,
		Volume:      rand.Int63n(1000000) + 10000, // 10k - 1M volume
		Bid:         bid,
		Ask:         ask,
		Timestamp:   time.Now().UnixNano(),
		TradeType:   []string{"BUY", "SELL"}[rand.Intn(2)],
		MarketDepth: rand.Int31n(500) + 50, // 50-550 orders
	}

	log.Printf("Serving %s: Price=$%.2f, Volume=%d",
		tick.Symbol, tick.Price, tick.Volume)

	return tick, nil
}

func main() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Listen on TCP port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create gRPC server
	s := grpc.NewServer()

	// Register our service
	pb.RegisterStockServiceServer(s, &server{})

	log.Println("🚀 gRPC Stock Service running on :50051")
	log.Println("Waiting for client connections...")

	// Start serving
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
