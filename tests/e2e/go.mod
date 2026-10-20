module github.com/astryon/e2e

go 1.22

require (
	github.com/astryon/event-contracts v0.0.0
	github.com/twmb/franz-go v1.18.1
	github.com/twmb/franz-go/pkg/kmsg v1.9.0
)

replace github.com/astryon/event-contracts => ../../libs/event-contracts
