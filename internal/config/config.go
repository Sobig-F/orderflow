package config

import "os"

type Config struct {
	DatabaseURL  string
	RedisURL     string
	KafkaBrokers string
	KafkaTopic   string
	Port         string
}

func Load() Config {
	return Config{
		DatabaseURL:  envOrDefault("DATABASE_URL", "postgres://orderflow:orderflow@localhost:5433/orderflow?sslmode=disable"),
		RedisURL:     envOrDefault("REDIS_URL", "redis://localhost:6379/0"),
		KafkaBrokers: envOrDefault("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   envOrDefault("KAFKA_TOPIC", "order-events"),
		Port:         envOrDefault("PORT", "8080"),
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
