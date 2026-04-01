package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	pollInterval = 100 * time.Millisecond
	httpTimeout  = 3 * time.Second
	rowFmt       = "  %7s │ %9s │ %9s │ %8s │ %4s │ %9s │ %9s │ %9s\n"
	placeholder  = "---"
)

// --- JSON response types ---

type blockHeader struct {
	ChainID string    `json:"chain_id"`
	Height  string    `json:"height"`
	Time    time.Time `json:"time"`
}

type blockData struct {
	Txs []string `json:"txs"`
}

type block struct {
	Header blockHeader `json:"header"`
	Data   blockData   `json:"data"`
}

type blockResponse struct {
	Result struct {
		Block block `json:"block"`
	} `json:"result"`
}

func (br *blockResponse) header() blockHeader { return br.Result.Block.Header }
func (br *blockResponse) txCount() int        { return len(br.Result.Block.Data.Txs) }

func (br *blockResponse) heightInt64() (int64, error) {
	return strconv.ParseInt(br.header().Height, 10, 64)
}

// --- monitor ---

type monitor struct {
	rpcAddr       string
	timeoutCommit string
	client        *http.Client

	count     int
	totalMs   float64
	minMs     float64
	maxMs     float64
	intervals []float64

	prevHeight    int64
	prevBlockTime time.Time
	prevObserved  time.Time
}

func newMonitor(rpcAddr, timeoutCommit string) *monitor {
	return &monitor{
		rpcAddr:       rpcAddr,
		timeoutCommit: timeoutCommit,
		client:        &http.Client{Timeout: httpTimeout},
		minMs:         math.MaxFloat64,
	}
}

func (m *monitor) run() {
	first, err := m.fetchLatestBlock()
	if err != nil {
		fmt.Fprintf(os.Stderr, "  [error] failed to connect RPC (%s): %v\n", m.rpcAddr, err)
		os.Exit(1)
	}

	printHeader(first.header().ChainID, m.rpcAddr, m.timeoutCommit)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sig:
			m.printSummary()
			return
		case <-ticker.C:
			m.poll()
		}
	}
}

func (m *monitor) poll() {
	block, err := m.fetchLatestBlock()
	if err != nil {
		return
	}

	height, err := block.heightInt64()
	if err != nil || height <= m.prevHeight {
		return
	}

	now := time.Now()
	bt := block.header().Time
	latency := now.Sub(bt)

	row := rowData{
		height:  fmt.Sprintf("%d", height),
		latency: fmtDuration(latency),
		txs:     fmt.Sprintf("%d", block.txCount()),
	}

	if m.prevHeight > 0 {
		interval := bt.Sub(m.prevBlockTime)
		observed := now.Sub(m.prevObserved)
		m.record(float64(interval.Milliseconds()))

		row.interval = fmtDuration(interval)
		row.observed = fmtDuration(observed)
		row.avg = fmtMs(m.totalMs / float64(m.count))
		row.min = fmtMs(m.minMs)
		row.max = fmtMs(m.maxMs)
	} else {
		row.interval, row.observed = placeholder, placeholder
		row.avg, row.min, row.max = placeholder, placeholder, placeholder
	}

	printRow(row)

	m.prevHeight = height
	m.prevBlockTime = bt
	m.prevObserved = now
}

func (m *monitor) record(ms float64) {
	m.count++
	m.totalMs += ms
	m.intervals = append(m.intervals, ms)
	if ms < m.minMs {
		m.minMs = ms
	}
	if ms > m.maxMs {
		m.maxMs = ms
	}
}

func (m *monitor) fetchLatestBlock() (*blockResponse, error) {
	resp, err := m.client.Get(m.rpcAddr + "/block")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var result blockResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (m *monitor) printSummary() {
	if m.count == 0 {
		fmt.Println("\n  No blocks recorded.")
		return
	}

	avgMs := m.totalMs / float64(m.count)
	stddev := calcStddev(m.intervals, avgMs)

	fmt.Printf("\n  ══════════════════════════════════════════\n")
	fmt.Printf("  Summary (%d blocks measured)\n", m.count)
	fmt.Printf("  ──────────────────────────────────────────\n")
	fmt.Printf("  Configured: timeout_commit = %s\n", m.timeoutCommit)
	fmt.Printf("  Average:    %s\n", fmtMs(avgMs))
	fmt.Printf("  Min:        %s\n", fmtMs(m.minMs))
	fmt.Printf("  Max:        %s\n", fmtMs(m.maxMs))
	fmt.Printf("  Stddev:     %s\n", fmtMs(stddev))
	fmt.Printf("  Total time: %s\n", fmtMs(m.totalMs))

	if configured, err := time.ParseDuration(m.timeoutCommit); err == nil {
		cfgMs := float64(configured.Milliseconds())
		diffMs := avgMs - cfgMs
		pct := (diffMs / cfgMs) * 100
		sign := "+"
		if diffMs < 0 {
			sign = ""
		}
		fmt.Printf("  Drift:      %s%.0fms (%s%.1f%% vs config)\n", sign, diffMs, sign, pct)
	}
	fmt.Printf("  ══════════════════════════════════════════\n\n")
}

// --- output helpers ---

type rowData struct {
	height, interval, observed, latency string
	txs, avg, min, max                  string
}

func printRow(r rowData) {
	fmt.Printf(rowFmt, r.height, r.interval, r.observed, r.latency, r.txs, r.avg, r.min, r.max)
}

func printHeader(chainID, rpcAddr, timeoutCommit string) {
	sep := func(w int) string { return strings.Repeat("─", w) }

	fmt.Println()
	fmt.Println("  Block Time Monitor")
	fmt.Printf("  Chain: %s  |  RPC: %s\n", chainID, rpcAddr)
	fmt.Printf("  Config: timeout_commit = %s\n", timeoutCommit)
	fmt.Println("  Press Ctrl+C to stop and view summary")
	fmt.Println()
	fmt.Printf(rowFmt, "Height", "Interval", "Observed", "Latency", "TXs", "Avg", "Min", "Max")
	fmt.Printf("  %s┼%s┼%s┼%s┼%s┼%s┼%s┼%s\n",
		sep(8), sep(11), sep(11), sep(10), sep(6), sep(11), sep(11), sep(11))
}

// --- config ---

func readTimeoutCommit(home string) string {
	f, err := os.Open(filepath.Join(home, "config", "config.toml"))
	if err != nil {
		return "(config.toml not found)"
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "timeout_commit") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			return strings.Trim(strings.TrimSpace(parts[1]), "\"")
		}
	}
	return "(not set)"
}

// --- formatting ---

func calcStddev(vals []float64, avg float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		d := v - avg
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(vals)))
}

func fmtDuration(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 0 {
		ms = -ms
	}
	if ms < 1000 {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}

func fmtMs(ms float64) string {
	if ms < 1000 {
		return fmt.Sprintf("%.0fms", ms)
	}
	return fmt.Sprintf("%.3fs", ms/1000)
}

// --- entrypoint ---

func main() {
	homeDir, _ := os.UserHomeDir()
	defaultHome := filepath.Join(homeDir, ".minid")

	home := flag.String("home", defaultHome, "node home directory")
	rpc := flag.String("rpc", "http://localhost:26657", "CometBFT RPC address")
	flag.Parse()

	m := newMonitor(*rpc, readTimeoutCommit(*home))
	m.run()
}
