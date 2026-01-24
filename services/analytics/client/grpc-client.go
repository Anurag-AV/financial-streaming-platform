package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/Anurag-AV/financial-streaming-platform/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:50053",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewAnalyticsServiceClient(conn)
	symbols := []string{"AAPL", "GOOGL", "MSFT", "TSLA"}

	fmt.Println("Analytics Engine Client - gRPC")
	fmt.Println("================================")
	fmt.Println()

	// Test 1: Portfolio Analytics
	fmt.Println("1. Portfolio Analytics")
	fmt.Println("----------------------")
	start := time.Now()
	portfolio, err := client.GetPortfolioAnalytics(context.Background(), &pb.AnalyticsRequest{
		Symbols: symbols,
	})
	latency := time.Since(start)
	
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("Symbols: %v\n", portfolio.Symbols)
		fmt.Printf("Portfolio Volatility: %.2f%%\n", portfolio.PortfolioVolatility)
		fmt.Printf("Sharpe Ratio: %.2f\n", portfolio.SharpeRatio)
		fmt.Printf("VaR (95%%): %.2f%%\n", portfolio.ValueAtRisk_95)
		fmt.Printf("VaR (99%%): %.2f%%\n", portfolio.ValueAtRisk_99)
		fmt.Printf("Latency: %v\n", latency)
	}
	fmt.Println()

	// Test 2: Risk Metrics
	fmt.Println("2. Risk Metrics")
	fmt.Println("---------------")
	start = time.Now()
	stream, err := client.GetRiskMetrics(context.Background(), &pb.AnalyticsRequest{
		Symbols: symbols,
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	for {
		metrics, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error: %v", err)
			break
		}

		fmt.Printf("%s:\n", metrics.Symbol)
		fmt.Printf("  Volatility: %.2f%%\n", metrics.Volatility)
		fmt.Printf("  Sharpe Ratio: %.2f\n", metrics.SharpeRatio)
		fmt.Printf("  VaR (95%%): %.2f%%\n", metrics.ValueAtRisk_95)
		fmt.Printf("  Max Drawdown: %.2f%%\n", metrics.MaxDrawdown)
		fmt.Printf("  Expected Return: %.2f%%\n", metrics.ExpectedReturn)
		fmt.Println()
	}
	fmt.Printf("Stream Latency: %v\n", time.Since(start))
	fmt.Println()

	// Test 3: Correlation Matrix
	fmt.Println("3. Correlation Matrix")
	fmt.Println("---------------------")
	start = time.Now()
	corrMatrix, err := client.GetCorrelationMatrix(context.Background(), &pb.AnalyticsRequest{
		Symbols: symbols,
	})
	latency = time.Since(start)
	
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, pair := range corrMatrix.Pairs {
			fmt.Printf("%s vs %s: %.3f (n=%d)\n", 
				pair.Symbol1, pair.Symbol2, pair.Correlation, pair.SampleSize)
		}
		fmt.Printf("Latency: %v\n", latency)
	}
	fmt.Println()

	// Test 4: Trading Signals
	fmt.Println("4. Trading Signals")
	fmt.Println("------------------")
	start = time.Now()
	signalStream, err := client.GetTradingSignals(context.Background(), &pb.AnalyticsRequest{
		Symbols: symbols,
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	for {
		signal, err := signalStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error: %v", err)
			break
		}

		fmt.Printf("%s: %s (confidence: %.0f%%)\n", 
			signal.Symbol, signal.SignalType, signal.Confidence*100)
		fmt.Printf("  Reason: %s\n", signal.Reason)
		fmt.Printf("  Target: $%.2f\n", signal.TargetPrice)
		fmt.Println()
	}
	fmt.Printf("Stream Latency: %v\n", time.Since(start))
	fmt.Println()

	// Test 5: Performance Metrics
	fmt.Println("5. Performance Metrics (gRPC)")
	fmt.Println("-----------------------------")
	perfMetrics, err := client.GetPerformanceMetrics(context.Background(), &pb.AnalyticsRequest{
		Symbols: symbols,
	})
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("Average Latency: %.2f ms\n", perfMetrics.AvgLatencyMs)
		fmt.Printf("P50 Latency: %.2f ms\n", perfMetrics.P50LatencyMs)
		fmt.Printf("P95 Latency: %.2f ms\n", perfMetrics.P95LatencyMs)
		fmt.Printf("P99 Latency: %.2f ms\n", perfMetrics.P99LatencyMs)
		fmt.Printf("Request Count: %d\n", perfMetrics.RequestCount)
		fmt.Printf("Throughput: %.2f rps\n", perfMetrics.ThroughputRps)
	}
}