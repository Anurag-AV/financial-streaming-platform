package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
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
	colorCyan   = "\033[36m"
	colorPurple = "\033[35m"
)

func main() {
	conn, err := grpc.Dial("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewStockServiceClient(conn)

	fmt.Println(colorPurple + "🔔 Price Alert System" + colorReset)
	fmt.Println(colorPurple + "=====================" + colorReset)
	fmt.Println()
	fmt.Println("Set price alerts and get notified when targets are hit!")
	fmt.Println()

	ctx := context.Background()

	// Start bidirectional stream
	stream, err := client.SubscribePriceAlerts(ctx)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Goroutine to receive alert notifications
	go func() {
		for {
			alert, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Printf("Error receiving alert: %v", err)
				return
			}

			// Display alert
			if alert.Message == "Alert registered successfully" {
				fmt.Printf("\n%s✓ Alert registered: %s %s $%.2f%s\n",
					colorGreen, alert.Symbol, alert.Condition, alert.TargetPrice, colorReset)
			} else {
				fmt.Printf("\n%s🚨 ALERT TRIGGERED!%s\n", colorRed, colorReset)
				fmt.Printf("%s   %s %s $%.2f%s\n",
					colorYellow, alert.Symbol, alert.Condition, alert.TargetPrice, colorReset)
				fmt.Printf("   Current Price: $%.2f\n", alert.CurrentPrice)
				fmt.Printf("   Time: %s\n", time.Unix(0, alert.Timestamp).Format(time.RFC3339))
			}
			fmt.Print("\n> ")
		}
	}()

	// Interactive prompt
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Available symbols: AAPL, GOOGL, MSFT, TSLA, NVDA, META, AMZN, NFLX, AMD, INTC")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  alert <SYMBOL> <ABOVE|BELOW> <PRICE>  - Set a price alert")
	fmt.Println("  price <SYMBOL>                         - Get current price")
	fmt.Println("  quit                                   - Exit")
	fmt.Println()

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		command := strings.ToLower(parts[0])

		switch command {
		case "quit", "exit", "q":
			fmt.Println("Goodbye!")
			return

		case "alert":
			if len(parts) != 4 {
				fmt.Println(colorRed + "Usage: alert <SYMBOL> <ABOVE|BELOW> <PRICE>" + colorReset)
				continue
			}

			symbol := strings.ToUpper(parts[1])
			condition := strings.ToUpper(parts[2])
			targetPrice, err := strconv.ParseFloat(parts[3], 64)

			if err != nil {
				fmt.Println(colorRed + "Invalid price format" + colorReset)
				continue
			}

			if condition != "ABOVE" && condition != "BELOW" {
				fmt.Println(colorRed + "Condition must be ABOVE or BELOW" + colorReset)
				continue
			}

			// Send alert request
			req := &pb.PriceAlertRequest{
				Symbol:      symbol,
				TargetPrice: targetPrice,
				Condition:   condition,
			}

			if err := stream.Send(req); err != nil {
				log.Printf("Error sending alert: %v", err)
				continue
			}

			fmt.Printf("Setting alert: %s %s $%.2f...\n", symbol, condition, targetPrice)

		case "price":
			if len(parts) != 2 {
				fmt.Println(colorRed + "Usage: price <SYMBOL>" + colorReset)
				continue
			}

			symbol := strings.ToUpper(parts[1])
			
			// Get current price
			tick, err := client.GetStockPrice(ctx, &pb.StockRequest{Symbol: symbol})
			if err != nil {
				fmt.Printf(colorRed+"Error getting price: %v"+colorReset+"\n", err)
				continue
			}

			changeColor := colorGreen
			if tick.ChangePercent < 0 {
				changeColor = colorRed
			}

			fmt.Printf("\n%s%s%s\n", colorCyan, symbol, colorReset)
			fmt.Printf("  Price:  $%.2f %s(%+.2f%%)%s\n", tick.Price, changeColor, tick.ChangePercent, colorReset)
			fmt.Printf("  Bid:    $%.2f\n", tick.Bid)
			fmt.Printf("  Ask:    $%.2f\n", tick.Ask)
			fmt.Printf("  High:   $%.2f\n", tick.DayHigh)
			fmt.Printf("  Low:    $%.2f\n", tick.DayLow)
			fmt.Printf("  Volume: %d\n", tick.Volume)
			fmt.Println()

		default:
			fmt.Println(colorRed + "Unknown command. Try 'alert', 'price', or 'quit'" + colorReset)
		}
	}
}