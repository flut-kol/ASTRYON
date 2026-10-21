package api

import (
	"context"
	"go-defender/pkg/domain/defender"
	"go-defender/pkg/domain/dto"
)

type Service interface {
	Ping(ctx context.Context) (dto.Ping, error)
	ScanFile(ctx context.Context, scanRequest dto.FileScanRequest) (dto.FileScanResponse, error)
	ScanFiles(ctx context.Context, scanRequest dto.FilesScanRequest) (dto.FilesScanResponse, error)
}

type Config struct {
	Defender defender.Defender
}

func MakeService(cfg Config) Service {
	return &service{
		defender: cfg.Defender,
	}
}

type service struct {
	defender defender.Defender
}

func (s *service) Ping(ctx context.Context) (dto.Ping, error) {
	isHealthy, err := s.defender.Ping(ctx)
	if err != nil {
		return dto.MakeUnhealthyPing(), err
	}

	return dto.Ping{IsHealthy: isHealthy}, nil
}

func (s *service) ScanFile(ctx context.Context, scanRequest dto.FileScanRequest) (dto.FileScanResponse, error) {
	output, err := s.defender.ScanFile(ctx, scanRequest.Name)
	if err != nil {
		return dto.FileScanResponse{Output: "", Error: err.Error()}, err
	}

	return dto.FileScanResponse{Output: output}, nil
}

func (s *service) ScanFiles(ctx context.Context, scanRequest dto.FilesScanRequest) (dto.FilesScanResponse, error) {
	results, err := s.defender.ScanFiles(ctx, scanRequest.Names)
	if err != nil {
		e := err.Error()
		return dto.FilesScanResponse{Results: []defender.FileScanResult{}, Error: &e}, err
	}

	return dto.FilesScanResponse{Results: results}, nil
}
