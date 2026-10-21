module github.com/astryon/replay-worker

go 1.27.0

require (
	github.com/astryon/event-contracts v0.0.0
	github.com/jackc/pgx/v5 v5.9.2
	github.com/twmb/franz-go v1.18.1
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/astryon/event-contracts => ../../libs/event-contracts
