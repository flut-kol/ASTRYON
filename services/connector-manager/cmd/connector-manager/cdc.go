package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/astryon/connector-manager/internal/cdc"
	"github.com/astryon/connector-manager/internal/kafkaemit"
)

// cdcSettings is the optional Debezium consumer, enabled by ASTRYON_CDC_TOPIC. Tenant and source come from this operator
// configuration (the connector definition), never from change payloads.
type cdcSettings struct {
	Topic, Group string
	Map          cdc.MapConfig
}

func loadCDC(getenv func(string) string) (*cdcSettings, error) {
	topic := strings.TrimSpace(getenv("ASTRYON_CDC_TOPIC"))
	if topic == "" {
		return nil, nil
	}
	s := &cdcSettings{Topic: topic, Group: getenv("ASTRYON_CDC_GROUP")}
	s.Map = cdc.MapConfig{ConnectorID: getenv("ASTRYON_CDC_CONNECTOR_ID"), Tenant: getenv("ASTRYON_CDC_TENANT_ID"),
		Source: getenv("ASTRYON_CDC_SOURCE_ID"), SchemaID: getenv("ASTRYON_CDC_SCHEMA_ID"), SchemaVersion: 1, ProcessingVersion: 1}
	if s.Group == "" {
		s.Group = "astryon-cdc-" + s.Map.ConnectorID
	}
	for _, kv := range []struct {
		name string
		dst  *int
	}{{"ASTRYON_CDC_SCHEMA_VERSION", &s.Map.SchemaVersion}, {"ASTRYON_CDC_PROCESSING_VERSION", &s.Map.ProcessingVersion}} {
		if v := getenv(kv.name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("%s must be a positive integer", kv.name)
			}
			*kv.dst = n
		}
	}
	for _, c := range strings.Split(getenv("ASTRYON_CDC_PRIMARY_KEY"), ",") {
		if c = strings.TrimSpace(c); c != "" {
			s.Map.PrimaryKey = append(s.Map.PrimaryKey, c)
		}
	}
	if s.Map.ConnectorID == "" {
		return nil, errors.New("ASTRYON_CDC_CONNECTOR_ID is required with ASTRYON_CDC_TOPIC")
	}
	if err := s.Map.Validate(time.Now()); err != nil {
		return nil, fmt.Errorf("ASTRYON_CDC_*: %w", err)
	}
	return s, nil
}

// startCDC launches the consumer loop; it stops when ctx ends. The returned func releases the Kafka clients.
func startCDC(ctx context.Context, s *cdcSettings, kc kafkaemit.Config, em cdc.Emitter) (func(), error) {
	conn, err := kc.ConnOpts()
	if err != nil {
		return nil, err
	}
	src, err := cdc.NewKafkaSource(conn, s.Group, s.Topic)
	if err != nil {
		return nil, err
	}
	q, err := cdc.NewKafkaQuarantine(conn)
	if err != nil {
		src.Close()
		return nil, err
	}
	c := &cdc.Consumer{Feed: src, Emitter: em, Quarantine: q, Map: s.Map}
	go func() {
		if err := c.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("cdc consumer: %v", err)
		}
	}()
	return func() { src.Close(); q.Close() }, nil
}
