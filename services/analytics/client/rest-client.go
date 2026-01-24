package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	pb "github.com/Anurag-AV/financial-streaming-platform/proto"
)

func main() {
	baseURL := "http://localhost:8080"
	// symbols := []string{"AAPL", "GOOGL", "MSFT", "TSLA"}

	fmt.Println("Analytics Engine Client - REST")
	fmt.Println("===============================")
	fmt.Println()

	// Test 1: Portfolio Analytics
	fmt.Println("1. Portfolio Analytics")
	fmt.Println("----------------------")
	start := time.Now()
	url := fmt.Sprintf("%s/api/portfolio?symbol=AAPL&symbol=GOOGL&symbol=MSFT&symbol=TSLA", baseURL)
	resp, err := http.Get(url)
	latency := time.Since(start)
	
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		
		var portfolio pb.PortfolioAnalytics
		json.Unmarshal(body, &portfolio)
		
		fmt.Printf("Symbols: %v\n", portfolio.Symbols)
		fmt.Printf("Portfolio Volatility: %.2f%%\n", portfolio.PortfolioVolatility)
		fmt.Printf("Sharpe Ratio: %.2f\n", portfolio.SharpeRatio)
		fmt.Printf("VaR (95%%): %.2f%%\n", portfolio.ValueAtRisk_95)
		fmt.Printf("VaR (99%%): %.2f%%\n", portfolio.ValueAtRisk_99)
		fmt.Printf("Latency: %v\n", latency)
		fmt.Printf("Payload Size: %d bytes\n", len(body))
	}
	fmt.Println()

	// Test 2: Risk Metrics
	fmt.Println("2. Risk Metrics")
	fmt.Println("---------------")
	start = time.Now()
	url = fmt.Sprintf("%s/api/risk?symbol=AAPL&symbol=GOOGL&symbol=MSFT&symbol=TSLA", baseURL)
	resp, err = http.Get(url)
	latency = time.Since(start)
	
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		
		var metrics []*pb.RiskMetrics
		json.Unmarshal(body, &metrics)
		
		for _, m := range metrics {
			fmt.Printf("%s:\n", m.Symbol)
			fmt.Printf("  Volatility: %.2f%%\n", m.Volatility)
			fmt.Printf("  Sharpe Ratio: %.2f\n", m.SharpeRatio)
			fmt.Printf("  VaR (95%%): %.2f%%\n", m.ValueAtRisk_95)
			fmt.Printf("  Max Drawdown: %.2f%%\n", m.MaxDrawdown)
			fmt.Printf("  Expected Return: %.2f%%\n", m.ExpectedReturn)
			fmt.Println()
		}
		fmt.Printf("Latency: %v\n", latency)
		fmt.Printf("Payload Size: %d bytes\n", len(body))
	}
	fmt.Println()

	// Test 3: Correlation Matrix
	fmt.Println("3. Correlation Matrix")
	fmt.Println("---------------------")
	start = time.Now()
	url = fmt.Sprintf("%s/api/correlation?symbol=AAPL&symbol=GOOGL&symbol=MSFT&symbol=TSLA", baseURL)
	resp, err = http.Get(url)
	latency = time.Since(start)
	
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		
		var corrMatrix pb.CorrelationMatrix
		json.Unmarshal(body, &corrMatrix)
		
		for _, pair := range corrMatrix.Pairs {
			fmt.Printf("%s vs %s: %.3f (n=%d)\n", 
				pair.Symbol1, pair.Symbol2, pair.Correlation, pair.SampleSize)
		}
		fmt.Printf("Latency: %v\n", latency)
		fmt.Printf("Payload Size: %d bytes\n", len(body))
	}
	fmt.Println()

	// Test 4: Performance Metrics
	fmt.Println("4. Performance Metrics (REST)")
	fmt.Println("-----------------------------")
	url = fmt.Sprintf("%s/api/performance", baseURL)
	resp, err = http.Get(url)
	
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		
		var perfMetrics pb.PerformanceMetrics
		json.Unmarshal(body, &perfMetrics)
		
		fmt.Printf("Average Latency: %.2f ms\n", perfMetrics.AvgLatencyMs)
		fmt.Printf("P50 Latency: %.2f ms\n", perfMetrics.P50LatencyMs)
		fmt.Printf("P95 Latency: %.2f ms\n", perfMetrics.P95LatencyMs)
		fmt.Printf("P99 Latency: %.2f ms\n", perfMetrics.P99LatencyMs)
		fmt.Printf("Request Count: %d\n", perfMetrics.RequestCount)
		fmt.Printf("Throughput: %.2f rps\n", perfMetrics.ThroughputRps)
	}
}