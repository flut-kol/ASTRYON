module github.com/astryon/file-ingestion

go 1.27.0

require (
	github.com/astryon/audit v0.0.0-00010101000000-000000000000
	github.com/astryon/go-common v0.0.0-00010101000000-000000000000
	github.com/astryon/retry-controller v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v5 v5.9.2
	github.com/parquet-go/parquet-go v0.26.4
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go v1.18.1 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/astryon/audit => ../../libs/audit

replace github.com/astryon/go-common => ../../libs/go-common

replace github.com/astryon/retry-controller => ../retry-controller
