package main

import "testing"

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadCDC(t *testing.T) {
	if s, err := loadCDC(env(nil)); s != nil || err != nil {
		t.Fatal("disabled without topic")
	}
	base := map[string]string{"ASTRYON_CDC_TOPIC": "pg.public.people", "ASTRYON_CDC_CONNECTOR_ID": "c1",
		"ASTRYON_CDC_TENANT_ID": "11111111-1111-4111-8111-111111111111", "ASTRYON_CDC_SOURCE_ID": "22222222-2222-4222-8222-222222222222",
		"ASTRYON_CDC_SCHEMA_ID": "cdc.people", "ASTRYON_CDC_PRIMARY_KEY": "id, org"}
	s, err := loadCDC(env(base))
	if err != nil || s.Group != "astryon-cdc-c1" || len(s.Map.PrimaryKey) != 2 {
		t.Fatalf("%+v %v", s, err)
	}
	base["ASTRYON_CDC_TENANT_ID"] = "x"
	if _, err := loadCDC(env(base)); err == nil {
		t.Fatal("bad tenant must fail")
	}
}
