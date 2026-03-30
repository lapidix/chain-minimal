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

type blockResponse struct {
	Result struct {
		Block struct {
			Header struct {
				ChainID string    `json:"chain_id"`
				Height  string    `json:"height"`
				Time    time.Time `json:"time"`
			} `json:"header"`
			Data struct {
				Txs []string `json:"txs"`
			} `json:"data"`
		} `json:"block"`
	} `json:"result"`
}

type stats struct {
	count          int
	totalMs        float64
	minMs          float64
	maxMs          float64
	intervals      []float64
	timeoutCommit  string
}

func main() {
	homeDir, _ := os.UserHomeDir()
	defaultHome := filepath.Join(homeDir, ".minid")

	home := flag.String("home", defaultHome, "node home directory")
	rpc := flag.String("rpc", "http://localhost:26657", "CometBFT RPC address")
	flag.Parse()

	timeoutCommit := readTimeoutCommit(*home)

	st := &stats{minMs: math.MaxFloat64, timeoutCommit: timeoutCommit}
	var prevHeight int64
	var prevBlockTime time.Time
	var prevObserved time.Time

	first, err := fetchLatestBlock(*rpc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  [error] RPC 연결 실패 (%s): %v\n", *rpc, err)
		os.Exit(1)
	}

	chainID := first.Result.Block.Header.ChainID
	printHeader(chainID, *rpc, timeoutCommit)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sig:
			printSummary(st)
			return
		case <-ticker.C:
			block, err := fetchLatestBlock(*rpc)
			if err != nil {
				continue
			}

			height, _ := strconv.ParseInt(block.Result.Block.Header.Height, 10, 64)
			if height <= prevHeight {
				continue
			}

			blockTime := block.Result.Block.Header.Time
			observed := time.Now()
			numTxs := len(block.Result.Block.Data.Txs)
			latency := observed.Sub(blockTime)

			if prevHeight > 0 {
				interval := blockTime.Sub(prevBlockTime)
				observedInterval := observed.Sub(prevObserved)

				intervalMs := float64(interval.Milliseconds())
				st.count++
				st.totalMs += intervalMs
				if intervalMs < st.minMs {
					st.minMs = intervalMs
				}
				if intervalMs > st.maxMs {
					st.maxMs = intervalMs
				}
				st.intervals = append(st.intervals, intervalMs)

				avgMs := st.totalMs / float64(st.count)

				fmt.Printf("  %7d │ %9s │ %9s │ %8s │ %4d │ %9s │ %9s │ %9s\n",
					height,
					fmtDuration(interval),
					fmtDuration(observedInterval),
					fmtDuration(latency),
					numTxs,
					fmtMs(avgMs),
					fmtMs(st.minMs),
					fmtMs(st.maxMs))
			} else {
				fmt.Printf("  %7d │ %9s │ %9s │ %8s │ %4d │ %9s │ %9s │ %9s\n",
					height, "---", "---", fmtDuration(latency), numTxs, "---", "---", "---")
			}

			prevHeight = height
			prevBlockTime = blockTime
			prevObserved = observed
		}
	}
}

func readTimeoutCommit(home string) string {
	configPath := filepath.Join(home, "config", "config.toml")
	f, err := os.Open(configPath)
	if err != nil {
		return "(config.toml not found)"
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "timeout_commit") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				val := strings.TrimSpace(parts[1])
				val = strings.Trim(val, "\"")
				return val
			}
		}
	}
	return "(not set)"
}

func printHeader(chainID, rpcAddr, timeoutCommit string) {
	fmt.Println()
	fmt.Printf("  Block Time Monitor\n")
	fmt.Printf("  Chain: %s  |  RPC: %s\n", chainID, rpcAddr)
	fmt.Printf("  Config: timeout_commit = %s\n", timeoutCommit)
	fmt.Printf("  Ctrl+C to stop and see summary\n\n")
	fmt.Printf("  %7s │ %9s │ %9s │ %8s │ %4s │ %9s │ %9s │ %9s\n",
		"Height", "Interval", "Observed", "Latency", "TXs", "Avg", "Min", "Max")
	fmt.Printf("  %s┼%s┼%s┼%s┼%s┼%s┼%s┼%s\n",
		strings.Repeat("─", 8),
		strings.Repeat("─", 11),
		strings.Repeat("─", 11),
		strings.Repeat("─", 10),
		strings.Repeat("─", 6),
		strings.Repeat("─", 11),
		strings.Repeat("─", 11),
		strings.Repeat("─", 11))
}

func fetchLatestBlock(rpcAddr string) (*blockResponse, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(rpcAddr + "/block")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result blockResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return &result, nil
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

func printSummary(st *stats) {
	if st.count == 0 {
		fmt.Println("\n  블록이 기록되지 않았습니다.")
		return
	}

	avgMs := st.totalMs / float64(st.count)

	var sumSqDiff float64
	for _, v := range st.intervals {
		diff := v - avgMs
		sumSqDiff += diff * diff
	}
	stddev := math.Sqrt(sumSqDiff / float64(st.count))

	fmt.Printf("\n  ══════════════════════════════════════════\n")
	fmt.Printf("  Summary (%d blocks measured)\n", st.count)
	fmt.Printf("  ──────────────────────────────────────────\n")
	fmt.Printf("  Configured: timeout_commit = %s\n", st.timeoutCommit)
	fmt.Printf("  Average:    %s\n", fmtMs(avgMs))
	fmt.Printf("  Min:        %s\n", fmtMs(st.minMs))
	fmt.Printf("  Max:        %s\n", fmtMs(st.maxMs))
	fmt.Printf("  Stddev:     %s\n", fmtMs(stddev))
	fmt.Printf("  Total time: %s\n", fmtMs(st.totalMs))

	configured, err := time.ParseDuration(st.timeoutCommit)
	if err == nil {
		cfgMs := float64(configured.Milliseconds())
		diffMs := avgMs - cfgMs
		diffPct := (diffMs / cfgMs) * 100
		sign := "+"
		if diffMs < 0 {
			sign = ""
		}
		fmt.Printf("  Drift:      %s%.0fms (%s%.1f%% vs config)\n", sign, diffMs, sign, diffPct)
	}
	fmt.Printf("  ══════════════════════════════════════════\n\n")
}
