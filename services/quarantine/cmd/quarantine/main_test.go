package main

import "testing"

func TestValidateAuthConfiguration(t *testing.T) {
	values := map[string]string{}
	getenv := func(key string) string { return values[key] }
	if err := validateAuthConfiguration(getenv); err != nil {
		t.Fatal(err)
	}
	values["QUARANTINE_DEV_TOKEN"] = "secret"
	values["QUARANTINE_DEV_TENANT"] = "00000000-0000-4000-8000-0000000000a1"
	if err := validateAuthConfiguration(getenv); err == nil {
		t.Fatal("dev auth accepted without explicit enable flag")
	}
	values["QUARANTINE_ENABLE_DEV_AUTH"] = "true"
	if err := validateAuthConfiguration(getenv); err != nil {
		t.Fatal(err)
	}
	values["AAS_BASE_URL"] = "https://aas.example"
	if err := validateAuthConfiguration(getenv); err == nil {
		t.Fatal("mixed AAS/dev auth accepted")
	}
	delete(values, "QUARANTINE_DEV_TOKEN")
	delete(values, "QUARANTINE_DEV_TENANT")
	delete(values, "QUARANTINE_ENABLE_DEV_AUTH")
	for _, key := range []string{"AAS_ISSUER", "AAS_AUDIENCE", "AAS_ENV", "AAS_ALLOWED_AZP"} {
		values[key] = "configured"
	}
	if err := validateAuthConfiguration(getenv); err != nil {
		t.Fatal(err)
	}
}
