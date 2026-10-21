package defender

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"go-defender/pkg/utils/log"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"
)

const (
	command = "./mpclient"
)

type Defender interface {
	Ping(ctx context.Context) (bool, error)
	ScanFile(ctx context.Context, path string) (string, error)
	ScanFiles(ctx context.Context, fileNames []string) ([]FileScanResult, error)
}

type defender struct {
	timeout      int64
	sharedFolder string
	cmd          commandContext
}

type Config struct {
	Timeout      int64
	SharedFolder string
}

type FileScanResult struct {
	FileName        string  `json:"FileName"`
	FullPath        string  `json:"FullPath"`
	IsEncrypted     bool    `json:"IsEncrypted"`
	IsInfected      bool    `json:"IsInfected"`
	IsSuspicious    bool    `json:"IsSuspicious"`
	InfectionReason *string `json:"InfectionReason"`
	Error           *string `json:"Error"`

	// 🆕 More Metadata
	FileSizeBytes   int64   `json:"FileSizeBytes"`
	FileHashMD5     string  `json:"FileHashMD5"`
	EngineVersion   *string `json:"EngineVersion"`
	ScanOutput      string  `json:"ScanOutput"`
	ScanStart       string  `json:"ScanStart"`
	ScanEnd         string  `json:"ScanEnd"`
	ScanDurationMs  int64   `json:"ScanDurationMs"`
	ExitCode        int     `json:"ExitCode"`
	CommandExecuted string  `json:"CommandExecuted"`
}

type commandContext func(ctx context.Context, name string, args ...string) *exec.Cmd

func MakeDefender(cfg Config) Defender {
	return &defender{
		timeout:      cfg.Timeout,
		sharedFolder: cfg.SharedFolder,
		cmd:          exec.CommandContext,
	}
}

func (d *defender) Ping(_ context.Context) (bool, error) {
	return true, nil
}

func (d *defender) ScanFile(ctx context.Context, fileName string) (string, error) {
	timedCtx, cancel := context.WithTimeout(ctx, time.Duration(d.timeout)*time.Second)
	defer cancel()

	if err := os.Chdir("/loadlibrary"); err != nil {
		log.L.Fatalf("can't chdir to /loadlibrary: %v", err)
	}

	if strings.HasPrefix(fileName, "/shared/") {
		fileName = strings.TrimPrefix(fileName, "/shared/")
	}

	c := d.cmd(timedCtx, command, path.Join(d.sharedFolder, fileName))
	output, err := c.CombinedOutput()

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return string(output), nil // threat found (normal)
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("command %s timed out", command)
		}
		return "", fmt.Errorf("command %s failed: %v", command, err)
	}

	return string(output), nil
}

func (d *defender) ScanFiles(ctx context.Context, fileNames []string) ([]FileScanResult, error) {
	output, err := d.scanFiles(ctx, fileNames)
	if err != nil {
		return nil, err
	}
	return d.parseFilesOutput(output, fileNames)
}

func (d *defender) scanFiles(ctx context.Context, fileNames []string) (string, error) {
	timedCtx, cancel := context.WithTimeout(ctx, time.Duration(d.timeout)*time.Second)
	defer cancel()

	if err := os.Chdir("/loadlibrary"); err != nil {
		log.L.Fatalf("can't chdir to /loadlibrary: %v", err)
	}

	var args []string
	for _, f := range fileNames {
		args = append(args, path.Join(d.sharedFolder, f))
	}

	c := d.cmd(timedCtx, command, args...)
	output, err := c.CombinedOutput()
	if err != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "", fmt.Errorf("command %s failed: %v", command, err)
	}

	return string(output), nil
}

func (d *defender) parseFilesOutput(output string, fileNames []string) ([]FileScanResult, error) {
	regexPath := regexp.MustCompile(`main\(\): Scanning`)
	indexes := regexPath.FindAllStringSubmatchIndex(output, -1)
	if len(indexes) == 0 {
		return nil, errors.New("no output returned by mpclient")
	}

	var results []FileScanResult
	for i := 0; i < len(indexes); i++ {
		start := indexes[i][0]
		end := len(output)
		if i+1 < len(indexes) {
			end = indexes[i+1][0]
		}
		res, err := d.parseFileOutput(output[start:end])
		if err != nil {
			log.L.Errorf("parse error: %v", err)
		}
			log.L.Errorf("parse result: %v", res)

		results = append(results, res)
	}

	return results, nil
}

func (d *defender) parseFileOutput(outputPart string) (FileScanResult, error) {
	startTime := time.Now()

	lines := strings.Split(outputPart, "\n")
	if len(lines) == 0 {
		return FileScanResult{}, errors.New("empty scan output")
	}

	// 🧩 Extract scanned path
	regexPath := regexp.MustCompile(`main\(\): Scanning (?P<FilePath>[^\n]+)...`)
	pathMatches := regexPath.FindStringSubmatch(outputPart)
	if len(pathMatches) < 2 {
		return FileScanResult{}, errors.New("could not extract file path")
	}
	fullPath := pathMatches[1]
	relPath := strings.TrimPrefix(strings.TrimPrefix(fullPath, d.sharedFolder), "/")

	// 🧮 Get file size and hash
	var size int64
	var hash string
	if fi, err := os.Stat(fullPath); err == nil {
		size = fi.Size()
		if f, err := os.Open(fullPath); err == nil {
			defer f.Close()
			h := md5.New()
			if _, err := io.Copy(h, f); err == nil {
				hash = hex.EncodeToString(h.Sum(nil))
			}
		}
	}

	// 🦠 Detect threat
	regexThreat := regexp.MustCompile(`(?i)Threat\s+([^\s]+)\s+identified`)
	threatMatches := regexThreat.FindStringSubmatch(outputPart)
	var threatReason *string
	infected := false
	if len(threatMatches) > 0 {
		tr := threatMatches[1]
		threatReason = &tr
		infected = true
	}

	// ⚠️ Suspicious detection for PE files
	suspicious := false
	if !infected && strings.Contains(strings.ToLower(outputPart), "scanning input") {
		if strings.HasSuffix(strings.ToLower(fullPath), ".exe") || strings.HasSuffix(strings.ToLower(fullPath), ".dll") {
			reason := "Binary scanned with no detection — flagged as suspicious"
			threatReason = &reason
			infected = true
			suspicious = true
		}
	}

	// 📦 Detect engine version (if available)
	var engineVersion *string
	if ev := regexp.MustCompile(`Engine version: ([0-9.]+)`).FindStringSubmatch(outputPart); len(ev) > 1 {
		engineVersion = &ev[1]
	}

	// 📦 Handle errors
	var errText *string
	if strings.Contains(outputPart, "failed to open file") {
		errMsg := "Failed to open file — possibly locked, corrupt, or missing"
		errText = &errMsg
	}

	endTime := time.Now()

	result := FileScanResult{
		FileName:        relPath,
		FullPath:        fullPath,
		FileSizeBytes:   size,
		FileHashMD5:     hash,
		IsEncrypted:     isEncrypted(lines),
		IsInfected:      infected,
		IsSuspicious:    suspicious,
		InfectionReason: threatReason,
		Error:           errText,
		EngineVersion:   engineVersion,
		ScanOutput:      strings.TrimSpace(outputPart),
		ScanStart:       startTime.Format(time.RFC3339),
		ScanEnd:         endTime.Format(time.RFC3339),
		ScanDurationMs:  endTime.Sub(startTime).Milliseconds(),
		CommandExecuted: fmt.Sprintf("%s %s", command, fullPath),
	}

	if infected {
		log.L.Warnf("❌ Threat or suspicious file: %s — %s", relPath, *threatReason)
	} else {
		log.L.Infof("✅ Clean: %s (%d bytes)", relPath, size)
	}

	return result, nil
}

func isEncrypted(lines []string) bool {
	for i := 0; i < len(lines) && i < 5; i++ {
		if strings.Contains(lines[i], "File is encrypted") {
			return true
		}
	}
	return false
}
