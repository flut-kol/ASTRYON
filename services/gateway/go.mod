module github.com/astryon/gateway

go 1.22

require (
	github.com/astryon/event-contracts v0.0.0
	github.com/astryon/go-common v0.0.0
	github.com/lestrrat-go/jwx/v2 v2.1.3
	github.com/twmb/franz-go v1.18.1
	github.com/twmb/franz-go/pkg/kmsg v1.9.0
)

require (
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.3.0 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/lestrrat-go/blackmagic v1.0.2 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/httprc v1.0.6 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
)

replace github.com/astryon/event-contracts => ../../libs/event-contracts

replace github.com/astryon/go-common => ../../libs/go-common
