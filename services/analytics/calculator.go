package main

import (
	"math"
	"sort"
	"sync"
)

// PriceHistory stores historical price data
type PriceHistory struct {
	prices     []float64
	returns    []float64 // Daily returns
	timestamps []int64
	mu         sync.RWMutex
}

// NewPriceHistory creates a new price history tracker
func NewPriceHistory() *PriceHistory {
	return &PriceHistory{
		prices:     make([]float64, 0, 1000),
		returns:    make([]float64, 0, 1000),
		timestamps: make([]int64, 0, 1000),
	}
}

// AddPrice adds a new price to history
func (ph *PriceHistory) AddPrice(price float64, timestamp int64) {
	ph.mu.Lock()
	defer ph.mu.Unlock()

	// Calculate return if we have previous price
	if len(ph.prices) > 0 {
		prevPrice := ph.prices[len(ph.prices)-1]
		if prevPrice > 0 {
			ret := (price - prevPrice) / prevPrice
			ph.returns = append(ph.returns, ret)
		}
	}

	ph.prices = append(ph.prices, price)
	ph.timestamps = append(ph.timestamps, timestamp)

	// Keep only last 1000 prices
	if len(ph.prices) > 1000 {
		ph.prices = ph.prices[1:]
		ph.timestamps = ph.timestamps[1:]
		if len(ph.returns) > 999 {
			ph.returns = ph.returns[1:]
		}
	}
}

// GetReturns returns all returns
func (ph *PriceHistory) GetReturns() []float64 {
	ph.mu.RLock()
	defer ph.mu.RUnlock()
	result := make([]float64, len(ph.returns))
	copy(result, ph.returns)
	return result
}

// GetPrices returns all prices
func (ph *PriceHistory) GetPrices() []float64 {
	ph.mu.RLock()
	defer ph.mu.RUnlock()
	result := make([]float64, len(ph.prices))
	copy(result, ph.prices)
	return result
}

// CalculateVolatility calculates annualized volatility
func (ph *PriceHistory) CalculateVolatility() float64 {
	returns := ph.GetReturns()
	if len(returns) < 2 {
		return 0
	}

	mean := 0.0
	for _, r := range returns {
		mean += r
	}
	mean /= float64(len(returns))

	variance := 0.0
	for _, r := range returns {
		diff := r - mean
		variance += diff * diff
	}
	variance /= float64(len(returns))

	stdDev := math.Sqrt(variance)

	// Annualize (assuming 252 trading days, prices every 500ms = 172,800 periods/day)
	// Simplified: use sqrt(periods per year)
	annualizationFactor := math.Sqrt(252.0)
	return stdDev * annualizationFactor * 100 // Return as percentage
}

// CalculateSharpeRatio calculates Sharpe ratio (risk-free rate = 0 for simplicity)
func (ph *PriceHistory) CalculateSharpeRatio() float64 {
	returns := ph.GetReturns()
	if len(returns) < 2 {
		return 0
	}

	// Calculate mean return
	meanReturn := 0.0
	for _, r := range returns {
		meanReturn += r
	}
	meanReturn /= float64(len(returns))

	// Calculate volatility of returns
	variance := 0.0
	for _, r := range returns {
		diff := r - meanReturn
		variance += diff * diff
	}
	variance /= float64(len(returns))
	stdDev := math.Sqrt(variance)

	if stdDev == 0 {
		return 0
	}

	// Annualize
	annualizedReturn := meanReturn * 252
	annualizedVolatility := stdDev * math.Sqrt(252)

	// Sharpe Ratio = (Return - RiskFreeRate) / Volatility
	// Assuming risk-free rate = 0 for simplicity
	return annualizedReturn / annualizedVolatility
}

// CalculateVaR calculates Value at Risk at given confidence level
func (ph *PriceHistory) CalculateVaR(confidenceLevel float64) float64 {
	returns := ph.GetReturns()
	if len(returns) < 10 {
		return 0
	}

	// Sort returns
	sortedReturns := make([]float64, len(returns))
	copy(sortedReturns, returns)
	sort.Float64s(sortedReturns)

	// Find percentile
	index := int(float64(len(sortedReturns)) * (1.0 - confidenceLevel))
	if index < 0 {
		index = 0
	}
	if index >= len(sortedReturns) {
		index = len(sortedReturns) - 1
	}

	// VaR is the negative of the percentile (loss is negative return)
	return -sortedReturns[index] * 100 // Return as percentage
}

// CalculateMaxDrawdown calculates maximum peak-to-trough decline
func (ph *PriceHistory) CalculateMaxDrawdown() float64 {
	prices := ph.GetPrices()
	if len(prices) < 2 {
		return 0
	}

	maxPrice := prices[0]
	maxDrawdown := 0.0

	for _, price := range prices {
		if price > maxPrice {
			maxPrice = price
		}
		drawdown := (maxPrice - price) / maxPrice
		if drawdown > maxDrawdown {
			maxDrawdown = drawdown
		}
	}

	return maxDrawdown * 100 // Return as percentage
}

// CalculateExpectedReturn calculates annualized expected return
func (ph *PriceHistory) CalculateExpectedReturn() float64 {
	returns := ph.GetReturns()
	if len(returns) < 2 {
		return 0
	}

	meanReturn := 0.0
	for _, r := range returns {
		meanReturn += r
	}
	meanReturn /= float64(len(returns))

	// Annualize
	return meanReturn * 252 * 100 // Return as percentage
}

// CalculateCorrelation calculates correlation between two price histories
func CalculateCorrelation(ph1, ph2 *PriceHistory) float64 {
	returns1 := ph1.GetReturns()
	returns2 := ph2.GetReturns()

	// Use minimum length
	n := len(returns1)
	if len(returns2) < n {
		n = len(returns2)
	}

	if n < 2 {
		return 0
	}

	// Calculate means
	mean1, mean2 := 0.0, 0.0
	for i := 0; i < n; i++ {
		mean1 += returns1[i]
		mean2 += returns2[i]
	}
	mean1 /= float64(n)
	mean2 /= float64(n)

	// Calculate covariance and standard deviations
	covariance := 0.0
	variance1 := 0.0
	variance2 := 0.0

	for i := 0; i < n; i++ {
		diff1 := returns1[i] - mean1
		diff2 := returns2[i] - mean2
		covariance += diff1 * diff2
		variance1 += diff1 * diff1
		variance2 += diff2 * diff2
	}

	stdDev1 := math.Sqrt(variance1)
	stdDev2 := math.Sqrt(variance2)

	if stdDev1 == 0 || stdDev2 == 0 {
		return 0
	}

	return covariance / (stdDev1 * stdDev2)
}

// PerformanceTracker tracks API performance metrics
type PerformanceTracker struct {
	latencies    []float64
	payloadSizes []int64
	requestCount int64
	mu           sync.Mutex
}

// NewPerformanceTracker creates a new tracker
func NewPerformanceTracker() *PerformanceTracker {
	return &PerformanceTracker{
		latencies:    make([]float64, 0, 10000),
		payloadSizes: make([]int64, 0, 10000),
	}
}

// RecordRequest records a request's metrics
func (pt *PerformanceTracker) RecordRequest(latencyMs float64, payloadBytes int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.latencies = append(pt.latencies, latencyMs)
	pt.payloadSizes = append(pt.payloadSizes, payloadBytes)
	pt.requestCount++

	// Keep only last 10000 requests
	if len(pt.latencies) > 10000 {
		pt.latencies = pt.latencies[1:]
		pt.payloadSizes = pt.payloadSizes[1:]
	}
}

// GetMetrics calculates performance metrics
func (pt *PerformanceTracker) GetMetrics() (avgLatency, p50, p95, p99, throughput float64, avgPayload int64, count int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if len(pt.latencies) == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}

	// Calculate average latency
	sum := 0.0
	for _, lat := range pt.latencies {
		sum += lat
	}
	avgLatency = sum / float64(len(pt.latencies))

	// Calculate percentiles
	sorted := make([]float64, len(pt.latencies))
	copy(sorted, pt.latencies)
	sort.Float64s(sorted)

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]

	// Calculate average payload
	payloadSum := int64(0)
	for _, size := range pt.payloadSizes {
		payloadSum += size
	}
	if len(pt.payloadSizes) > 0 {
		avgPayload = payloadSum / int64(len(pt.payloadSizes))
	}

	// Rough throughput estimate (requests per second)
	// Assuming recent requests represent current throughput
	if avgLatency > 0 {
		throughput = 1000.0 / avgLatency // Rough estimate
	}

	count = pt.requestCount

	return
}
