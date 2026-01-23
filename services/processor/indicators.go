package main

import (
	"math"
	"sync"
)

// RingBuffer - circular buffer for efficient sliding window
type RingBuffer struct {
	data  []float64
	size  int
	index int
	count int
	mu    sync.RWMutex
}

// NewRingBuffer creates a new ring buffer
func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		data:  make([]float64, size),
		size:  size,
		index: 0,
		count: 0,
	}
}

// Add adds a value to the ring buffer
func (rb *RingBuffer) Add(value float64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.data[rb.index] = value
	rb.index = (rb.index + 1) % rb.size
	if rb.count < rb.size {
		rb.count++
	}
}

// Values returns all values in order (oldest to newest)
func (rb *RingBuffer) Values() []float64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return []float64{}
	}

	result := make([]float64, rb.count)
	if rb.count < rb.size {
		// Buffer not full yet
		copy(result, rb.data[:rb.count])
	} else {
		// Buffer is full, need to reconstruct order
		copy(result, rb.data[rb.index:])
		copy(result[rb.size-rb.index:], rb.data[:rb.index])
	}
	return result
}

// Mean calculates the average
func (rb *RingBuffer) Mean() float64 {
	values := rb.Values()
	if len(values) == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// StdDev calculates standard deviation
func (rb *RingBuffer) StdDev() float64 {
	values := rb.Values()
	if len(values) < 2 {
		return 0
	}

	mean := rb.Mean()
	variance := 0.0
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(len(values))
	return math.Sqrt(variance)
}

// Count returns number of elements
func (rb *RingBuffer) Count() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.count
}

// PriceVolumeBuffer - stores price and volume together
type PriceVolumeBuffer struct {
	prices  *RingBuffer
	volumes *RingBuffer
	mu      sync.RWMutex
}

// NewPriceVolumeBuffer creates a new price-volume buffer
func NewPriceVolumeBuffer(size int) *PriceVolumeBuffer {
	return &PriceVolumeBuffer{
		prices:  NewRingBuffer(size),
		volumes: NewRingBuffer(size),
	}
}

// Add adds a price-volume pair
func (pvb *PriceVolumeBuffer) Add(price float64, volume int64) {
	pvb.mu.Lock()
	defer pvb.mu.Unlock()

	pvb.prices.Add(price)
	pvb.volumes.Add(float64(volume))
}

// VWAP calculates Volume Weighted Average Price
func (pvb *PriceVolumeBuffer) VWAP() float64 {
	pvb.mu.RLock()
	defer pvb.mu.RUnlock()

	prices := pvb.prices.Values()
	volumes := pvb.volumes.Values()

	if len(prices) == 0 {
		return 0
	}

	priceVolume := 0.0
	totalVolume := 0.0

	for i := 0; i < len(prices); i++ {
		priceVolume += prices[i] * volumes[i]
		totalVolume += volumes[i]
	}

	if totalVolume == 0 {
		return 0
	}

	return priceVolume / totalVolume
}

// IndicatorCalculator - calculates all technical indicators
type IndicatorCalculator struct {
	symbol string

	// Buffers for different time windows
	// Assuming 500ms ticks: 600 ticks = 5 min, 1800 ticks = 15 min
	buffer5min  *PriceVolumeBuffer
	buffer15min *PriceVolumeBuffer
	priceBuffer *RingBuffer // For Bollinger Bands

	// EMA state
	ema5min      float64
	ema15min     float64
	ema5minInit  bool
	ema15minInit bool

	// Statistics
	tickCount       int64
	anomalyCount    int64
	lastPrice       float64
	priceHistory    *RingBuffer // Last 20 prices for anomaly detection
	processingTimes *RingBuffer // Track processing performance

	mu sync.RWMutex
}

// NewIndicatorCalculator creates a new calculator
func NewIndicatorCalculator(symbol string) *IndicatorCalculator {
	return &IndicatorCalculator{
		symbol:          symbol,
		buffer5min:      NewPriceVolumeBuffer(600),  // 5 minutes at 500ms
		buffer15min:     NewPriceVolumeBuffer(1800), // 15 minutes
		priceBuffer:     NewRingBuffer(600),
		priceHistory:    NewRingBuffer(20),
		processingTimes: NewRingBuffer(100),
		ema5min:         0,
		ema15min:        0,
		ema5minInit:     false,
		ema15minInit:    false,
		tickCount:       0,
		anomalyCount:    0,
	}
}

// CalculateSMA - Simple Moving Average
func (ic *IndicatorCalculator) CalculateSMA(buffer *RingBuffer) float64 {
	return buffer.Mean()
}

// CalculateEMA - Exponential Moving Average
func (ic *IndicatorCalculator) CalculateEMA(price float64, prevEMA float64, period int, initialized bool) (float64, bool) {
	if !initialized {
		return price, true
	}

	k := 2.0 / float64(period+1)
	ema := price*k + prevEMA*(1-k)
	return ema, true
}

// CalculateBollingerBands - Bollinger Bands (SMA ± 2*StdDev)
func (ic *IndicatorCalculator) CalculateBollingerBands() (upper, middle, lower float64) {
	middle = ic.priceBuffer.Mean()
	stdDev := ic.priceBuffer.StdDev()

	upper = middle + 2*stdDev
	lower = middle - 2*stdDev

	return upper, middle, lower
}

// DetectAnomaly - detect unusual price movements
func (ic *IndicatorCalculator) DetectAnomaly(currentPrice float64) (bool, string, float64) {
	if ic.priceHistory.Count() < 10 {
		return false, "", 0.0
	}

	mean := ic.priceHistory.Mean()
	stdDev := ic.priceHistory.StdDev()

	if stdDev == 0 {
		return false, "", 0.0
	}

	// Z-score: how many standard deviations away?
	zScore := math.Abs(currentPrice-mean) / stdDev

	// Anomaly if more than 3 standard deviations
	if zScore > 3.0 {
		anomalyType := "SPIKE"
		if currentPrice < mean {
			anomalyType = "DROP"
		}

		// Normalize score to 0-1
		score := math.Min(zScore/10.0, 1.0)

		return true, anomalyType, score
	}

	// Check for high volatility
	if stdDev/mean > 0.03 { // 3% volatility threshold
		return true, "VOLATILITY", stdDev / mean
	}

	return false, "", 0.0
}

// CalculateVolatility - recent price volatility
func (ic *IndicatorCalculator) CalculateVolatility() float64 {
	if ic.priceHistory.Count() < 2 {
		return 0
	}

	stdDev := ic.priceHistory.StdDev()
	mean := ic.priceHistory.Mean()

	if mean == 0 {
		return 0
	}

	// Return as percentage
	return (stdDev / mean) * 100
}

// CalculateMomentum - rate of price change
func (ic *IndicatorCalculator) CalculateMomentum() float64 {
	values := ic.priceHistory.Values()
	if len(values) < 2 {
		return 0
	}

	// Simple momentum: (current - oldest) / oldest
	oldest := values[0]
	current := values[len(values)-1]

	if oldest == 0 {
		return 0
	}

	return ((current - oldest) / oldest) * 100
}

// Update - process new tick and calculate all indicators
func (ic *IndicatorCalculator) Update(price float64, volume int64) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	// Add to buffers
	ic.buffer5min.Add(price, volume)
	ic.buffer15min.Add(price, volume)
	ic.priceBuffer.Add(price)
	ic.priceHistory.Add(price)

	// Update EMAs
	ic.ema5min, ic.ema5minInit = ic.CalculateEMA(price, ic.ema5min, 600, ic.ema5minInit)
	ic.ema15min, ic.ema15minInit = ic.CalculateEMA(price, ic.ema15min, 1800, ic.ema15minInit)

	ic.lastPrice = price
	ic.tickCount++
}

// GetIndicators - get all calculated indicators
func (ic *IndicatorCalculator) GetIndicators() (vwap, sma5, sma15, ema5, ema15, bUpper, bMiddle, bLower, volatility, momentum float64) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	vwap = ic.buffer5min.VWAP()
	sma5 = ic.buffer5min.prices.Mean()
	sma15 = ic.buffer15min.prices.Mean()
	ema5 = ic.ema5min
	ema15 = ic.ema15min
	bUpper, bMiddle, bLower = ic.CalculateBollingerBands()
	volatility = ic.CalculateVolatility()
	momentum = ic.CalculateMomentum()

	return
}
