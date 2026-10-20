package api

import (
	"fmt"
	"strconv"
	"strings"
)

// Config is loaded from environment only. Secrets have no defaults.
type Config struct {
	Addr string

	DevAuth     bool
	DevToken    string
	DevTenantID string
	DevScopes   []string

	AASBaseURL  string // AAS_BASE_URL
	AASAudience string // ASTRYON_AAS_AUDIENCE

	CHURL, CHDatabase, CHUser, CHPassword string

	PostgresDSN string // DATABASE_URL

	// CursorKey signs pagination cursors (>= 32 bytes). Must be identical on all replicas.
	CursorKey []byte

	RateLimitRPS   float64
	RateLimitBurst int
}

func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{
		Addr:        def(getenv("ASTRYON_QUERY_ADDR"), ":8083"),
		DevAuth:     getenv("ASTRYON_DEV_AUTH") == "1",
		DevToken:    getenv("ASTRYON_DEV_TOKEN"),
		DevTenantID: getenv("ASTRYON_DEV_TENANT_ID"),
		DevScopes: strings.Fields(strings.ReplaceAll(def(getenv("ASTRYON_DEV_SCOPES"),
			ScopeEventsRead+" "+ScopeMetricsRead+" "+ScopeSourcesRead+" "+ScopeJobsRead), ",", " ")),
		AASBaseURL:     getenv("AAS_BASE_URL"),
		AASAudience:    getenv("ASTRYON_AAS_AUDIENCE"),
		CHURL:          getenv("ASTRYON_CH_URL"),
		CHDatabase:     def(getenv("ASTRYON_CH_DATABASE"), "astryon"),
		CHUser:         getenv("ASTRYON_CH_USER"),
		CHPassword:     getenv("ASTRYON_CH_PASSWORD"),
		PostgresDSN:    getenv("DATABASE_URL"),
		CursorKey:      []byte(getenv("ASTRYON_QUERY_CURSOR_KEY")),
		RateLimitRPS:   20,
		RateLimitBurst: 40,
	}
	if v := getenv("ASTRYON_QUERY_RATE_RPS"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 {
			return c, fmt.Errorf("ASTRYON_QUERY_RATE_RPS invalid")
		}
		c.RateLimitRPS = f
	}
	if v := getenv("ASTRYON_QUERY_RATE_BURST"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return c, fmt.Errorf("ASTRYON_QUERY_RATE_BURST invalid")
		}
		c.RateLimitBurst = n
	}
	if c.DevAuth {
		if c.DevToken == "" || c.DevTenantID == "" {
			return c, fmt.Errorf("ASTRYON_DEV_AUTH=1 requires ASTRYON_DEV_TOKEN and ASTRYON_DEV_TENANT_ID")
		}
	} else if c.AASBaseURL == "" || c.AASAudience == "" {
		return c, fmt.Errorf("AAS_BASE_URL and ASTRYON_AAS_AUDIENCE are required (or ASTRYON_DEV_AUTH=1)")
	}
	if len(c.CursorKey) < 32 {
		return c, fmt.Errorf("ASTRYON_QUERY_CURSOR_KEY must be at least 32 bytes")
	}
	if c.CHURL == "" {
		return c, fmt.Errorf("ASTRYON_CH_URL is required")
	}
	return c, nil
}

func def(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
