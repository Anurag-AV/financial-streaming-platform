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
	conn, err := grpc.Dial("localhost:50052",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewProcessorServiceClient(conn)

	symbols := []string{"AAPL", "GOOGL", "MSFT", "TSLA"}

	fmt.Println(colorPurple + "📊 Stream Processor Client - Technical Indicators" + colorReset)
	fmt.Println(colorPurple + "==================================================" + colorReset)
	fmt.Printf("Streaming enriched data for: %v\n", symbols)
	fmt.Println("Press Ctrl+C to stop...")
	fmt.Println()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println(colorYellow + "\n🛑 Shutting down..." + colorReset)
		cancel()
	}()

	// Start streaming enriched data
	stream, err := client.StreamEnrichedData(ctx, &pb.ProcessorStreamRequest{
		Symbols:         symbols,
		IncludeRawTicks: false,
	})
	if err != nil {
		log.Fatalf("Stream failed: %v", err)
	}

	tickCount := 0

	for {
		enriched, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Error: %v", err)
			break
		}

		tickCount++
		tick := enriched.Tick

		// Color based on anomaly
		anomalyColor := colorReset
		anomalyIcon := "  "
		if enriched.IsAnomaly {
			anomalyColor = colorRed
			anomalyIcon = "🚨"
		}

		// Price change color
		changeColor := colorReset
		if tick.ChangePercent > 0 {
			changeColor = colorGreen
		} else if tick.ChangePercent < 0 {
			changeColor = colorRed
		}

		timestamp := time.Unix(0, tick.Timestamp).Format("15:04:05")

		fmt.Printf("[%s] %s %s%-6s%s | Price: $%s%-8.2f%s | VWAP: $%-8.2f | SMA5: $%-8.2f | EMA5: $%-8.2f\n",
			timestamp,
			anomalyIcon,
			colorBlue, tick.Symbol, colorReset,
			changeColor, tick.Price, colorReset,
			enriched.Vwap,
			enriched.Sma_5Min,
			enriched.Ema_5Min,
		)

		// Show detailed info for anomalies
		if enriched.IsAnomaly {
			fmt.Printf("     %s⚠️  ANOMALY DETECTED: %s (score: %.2f, volatility: %.2f%%)%s\n",
				anomalyColor,
				enriched.AnomalyType,
				enriched.AnomalyScore,
				enriched.Volatility,
				colorReset,
			)
		}

		// Every 20 ticks, show Bollinger Bands
		if tickCount%20 == 0 {
			fmt.Printf("\n%s📊 Bollinger Bands Update:%s\n", colorCyan, colorReset)
			for _, sym := range symbols {
				if enriched.Tick.Symbol == sym {
					fmt.Printf("   %s: Upper=$%.2f | Middle=$%.2f | Lower=$%.2f\n",
						sym,
						enriched.BollingerUpper,
						enriched.BollingerMiddle,
						enriched.BollingerLower,
					)
				}
			}
			fmt.Printf("   Momentum: %+.2f%% | Volatility: %.2f%%\n\n",
				enriched.PriceMomentum,
				enriched.Volatility,
			)
		}
	}

	fmt.Printf("\n%s✅ Stream completed. Total enriched ticks: %d%s\n",
		colorGreen, tickCount, colorReset)
}