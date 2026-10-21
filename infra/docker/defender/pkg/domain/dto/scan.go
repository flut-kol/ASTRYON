package dto

import "go-defender/pkg/domain/defender"

type FileScanRequest struct {
	Name    string `json:"FileName"`
	TraceId string `json:"TraceId"`
}

type FilesScanRequest struct {
	Names   []string `json:"FileNames"`
	TraceId string   `json:"TraceId"`
}

type FileScanResponse struct {
	Output string `json:"Output"`
	Error  string `json:"Error"`
}

type FilesScanResponse struct {
	Results []defender.FileScanResult `json:"Results"`
	Error   *string                   `json:"Error"`
}
