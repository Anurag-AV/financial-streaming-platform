package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/Anurag-AV/financial-streaming-platform/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ANSI color codes for terminal output
const (
	colorReset  = "\033[0m"
	colorGreen  = "\033[32m"
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
)

func main() {
	conn, err := grpc.Dial("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewStockServiceClient(conn)

	symbols := []string{"AAPL", "GOOGL", "MSFT", "TSLA", "NVDA", "META"}

	fmt.Println(colorCyan + "📈 Financial Data Streaming Platform - Live Client" + colorReset)
	fmt.Println(colorCyan + "====================================================" + colorReset)
	fmt.Printf("Streaming: %v\n", symbols)
	fmt.Println("Press Ctrl+C to stop...")
	fmt.Println()

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println(colorYellow + "\n🛑 Shutting down gracefully..." + colorReset)
		cancel()
	}()

	// Start streaming
	stream, err := client.StreamMarketData(ctx, &pb.StreamRequest{
		Symbols:          symbols,
		UpdateIntervalMs: 500, // Update every 500ms
	})
	if err != nil {
		log.Fatalf("Stream failed: %v", err)
	}

	// Stats tracking
	tickCount := 0
	priceMap := make(map[string]float64)
	startTime := time.Now()

	// Receive and display ticks
	for {
		tick, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				break // Context cancelled
			}
			log.Printf("Error: %v", err)
			break
		}

		tickCount++
		priceMap[tick.Symbol] = tick.Price

		// Color based on price change
		changeColor := colorReset
		if tick.ChangePercent > 0 {
			changeColor = colorGreen
		} else if tick.ChangePercent < 0 {
			changeColor = colorRed
		}

		timestamp := time.Unix(0, tick.Timestamp).Format("15:04:05.000")
		// spread := tick.Ask - tick.Bid
		// spreadPct := (spread / tick.Price) * 100

		fmt.Printf("[%s] %s%-6s%s | $%s%-9.2f%s | %sΔ%+6.2f%%%s | H: $%-7.2f L: $%-7.2f | Bid: $%-8.2f Ask: $%-8.2f | Vol: %-7d\n",
			timestamp,
			colorBlue, tick.Symbol, colorReset,
			changeColor, tick.Price, colorReset,
			changeColor, tick.ChangePercent, colorReset,
			tick.DayHigh,
			tick.DayLow,
			tick.Bid,
			tick.Ask,
			tick.Volume,
		)

		// Every 30 ticks, print summary
		if tickCount%30 == 0 {
			elapsed := time.Since(startTime)
			throughput := float64(tickCount) / elapsed.Seconds()
			
			fmt.Printf("\n%s📊 Statistics:%s\n", colorPurple, colorReset)
			fmt.Printf("   Ticks received: %d\n", tickCount)
			fmt.Printf("   Throughput: %.0f ticks/sec\n", throughput)
			fmt.Printf("   Current prices:\n")
			for sym, price := range priceMap {
				fmt.Printf("      %s: $%.2f\n", sym, price)
			}
			fmt.Println()
		}
	}

	elapsed := time.Since(startTime)
	avgThroughput := float64(tickCount) / elapsed.Seconds()

	fmt.Printf("\n%s✅ Stream Summary:%s\n", colorGreen, colorReset)
	fmt.Printf("   Total ticks: %d\n", tickCount)
	fmt.Printf("   Duration: %v\n", elapsed.Round(time.Second))
	fmt.Printf("   Average throughput: %.0f ticks/sec\n", avgThroughput)
}