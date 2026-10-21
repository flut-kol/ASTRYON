module github.com/astryon/quarantine

go 1.25.0

require (
	github.com/astryon/go-common v0.0.0
	github.com/twmb/franz-go v1.18.1
)

require (
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
)

replace github.com/astryon/go-common => ../../libs/go-common
