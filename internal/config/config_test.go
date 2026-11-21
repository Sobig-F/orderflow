package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("PORT", "")

	got := Load()
	if got.DatabaseURL != "postgres://orderflow:orderflow@localhost:5433/orderflow?sslmode=disable" {
		t.Errorf("DatabaseURL = %q", got.DatabaseURL)
	}
	if got.RedisURL != "redis://localhost:6379/0" || got.KafkaBrokers != "localhost:9092" || got.KafkaTopic != "order-events" || got.Port != "8080" {
		t.Errorf("unexpected defaults: %+v", got)
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://db.example/orderflow")
	t.Setenv("REDIS_URL", "redis://cache.example:6380/2")
	t.Setenv("KAFKA_BROKERS", "broker-a:9092,broker-b:9092")
	t.Setenv("KAFKA_TOPIC", "custom-events")
	t.Setenv("PORT", "9000")

	got := Load()
	if got.DatabaseURL != "postgres://db.example/orderflow" || got.RedisURL != "redis://cache.example:6380/2" || got.KafkaBrokers != "broker-a:9092,broker-b:9092" || got.KafkaTopic != "custom-events" || got.Port != "9000" {
		t.Fatalf("unexpected environment config: %+v", got)
	}
}
