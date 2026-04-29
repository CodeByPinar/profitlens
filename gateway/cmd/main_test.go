package main

import "testing"

func TestRequiredEnvReturnsValue(t *testing.T) {
	t.Setenv("JWT_SECRET", "local-test-secret")

	got, err := requiredEnv("JWT_SECRET")
	if err != nil {
		t.Fatalf("requiredEnv returned error: %v", err)
	}
	if got != "local-test-secret" {
		t.Fatalf("requiredEnv returned %q, want %q", got, "local-test-secret")
	}
}

func TestRequiredEnvFailsWhenMissing(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	if _, err := requiredEnv("JWT_SECRET"); err == nil {
		t.Fatal("requiredEnv returned nil error for missing JWT_SECRET")
	}
}

func TestEnvReturnsFallback(t *testing.T) {
	t.Setenv("OPTIONAL_SETTING", "")

	got := env("OPTIONAL_SETTING", "fallback")
	if got != "fallback" {
		t.Fatalf("env returned %q, want fallback", got)
	}
}
