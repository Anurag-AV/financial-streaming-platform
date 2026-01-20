package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/Anurag-AV/financial-streaming-platform/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to server
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create client
	client := pb.NewStockServiceClient(conn)

	// Stocks to query
	symbols := []string{"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"}

	fmt.Println(" Financial Data Streaming Platform - Client")
	fmt.Println("==============================================")
	fmt.Println()

	for _, symbol := range symbols {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		// Make RPC call
		tick, err := client.GetStockPrice(ctx, &pb.StockRequest{Symbol: symbol})

		if err != nil {
			log.Printf(" Error getting %s: %v", symbol, err)
			cancel()
			continue
		}

		fmt.Printf("%s\n", tick.Symbol)
		fmt.Printf("   Price:       $%.2f\n", tick.Price)
		fmt.Printf("   Bid/Ask:     $%.2f / $%.2f\n", tick.Bid, tick.Ask)
		fmt.Printf("   Spread:      $%.4f (%.3f%%)\n",
			tick.Ask-tick.Bid,
			((tick.Ask-tick.Bid)/tick.Price)*100)
		fmt.Printf("   Volume:      %d\n", tick.Volume)
		fmt.Printf("   Trade Type:  %s\n", tick.TradeType)
		fmt.Printf("   Market Depth: %d orders\n", tick.MarketDepth)
		fmt.Printf("   Timestamp:   %s\n", time.Unix(0, tick.Timestamp).Format(time.RFC3339))
		fmt.Println()

		cancel()

		// Small delay between requests
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("All stock prices retrieved successfully!")
}
