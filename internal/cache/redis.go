package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sobig-F/orderflow/internal/order"
)

var ErrMiss = errors.New("cache miss")

type Redis struct {
	client *redis.Client
}

func NewRedis(ctx context.Context, address string) (*Redis, error) {
	options, err := redis.ParseURL(address)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}

	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &Redis{client: client}, nil
}

func (r *Redis) GetOrder(ctx context.Context, id string) (*order.Order, error) {
	payload, err := r.client.Get(ctx, "order:"+id).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrMiss
	}
	if err != nil {
		return nil, fmt.Errorf("read order cache: %w", err)
	}

	var result order.Order
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, fmt.Errorf("decode cached order: %w", err)
	}
	return &result, nil
}

func (r *Redis) SetOrder(ctx context.Context, value *order.Order) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode order cache: %w", err)
	}
	if err := r.client.Set(ctx, "order:"+value.ID, payload, 5*time.Minute).Err(); err != nil {
		return fmt.Errorf("write order cache: %w", err)
	}
	return nil
}

func (r *Redis) InvalidateOrder(ctx context.Context, id string) error {
	if err := r.client.Del(ctx, "order:"+id).Err(); err != nil {
		return fmt.Errorf("invalidate order cache: %w", err)
	}
	return nil
}

func (r *Redis) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *Redis) Close() error {
	return r.client.Close()
}
