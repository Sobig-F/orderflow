package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/Sobig-F/orderflow/internal/order"
)

func TestRedisCacheLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	client, err := NewRedis(context.Background(), "redis://"+server.Addr()+"/0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetOrder(context.Background(), "order-1"); !errors.Is(err, ErrMiss) {
		t.Fatalf("missing key error = %v, want ErrMiss", err)
	}

	want := &order.Order{ID: "order-1", Amount: "42.00", Currency: "RUB", Status: "pending"}
	if err := client.SetOrder(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := client.GetOrder(context.Background(), want.ID)
	if err != nil || got.ID != want.ID || got.Amount != want.Amount {
		t.Fatalf("got=%+v err=%v", got, err)
	}

	server.FastForward(6 * 60 * 1000000000)
	if _, err := client.GetOrder(context.Background(), want.ID); !errors.Is(err, ErrMiss) {
		t.Fatalf("expired key error = %v, want ErrMiss", err)
	}
	if err := client.SetOrder(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	if err := client.InvalidateOrder(context.Background(), want.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetOrder(context.Background(), want.ID); !errors.Is(err, ErrMiss) {
		t.Fatalf("invalidated key error = %v, want ErrMiss", err)
	}

	client.client.Set(context.Background(), "order:broken", "{", 0)
	if _, err := client.GetOrder(context.Background(), "broken"); err == nil {
		t.Fatal("expected corrupt cached JSON error")
	}
}

func TestRedisConfigurationAndConnectionErrors(t *testing.T) {
	if _, err := NewRedis(context.Background(), "not-a-url"); err == nil {
		t.Fatal("expected invalid URL error")
	}

	server := miniredis.RunT(t)
	address := server.Addr()
	server.Close()
	if _, err := NewRedis(context.Background(), "redis://"+address+"/0"); err == nil {
		t.Fatal("expected ping error for unavailable Redis")
	}

	server = miniredis.RunT(t)
	client, err := NewRedis(context.Background(), "redis://"+server.Addr()+"/0")
	if err != nil {
		t.Fatal(err)
	}
	server.Close()
	if _, err := client.GetOrder(context.Background(), "id"); err == nil {
		t.Error("expected Redis read error")
	}
	if err := client.SetOrder(context.Background(), &order.Order{ID: "id"}); err == nil {
		t.Error("expected Redis write error")
	}
	if err := client.InvalidateOrder(context.Background(), "id"); err == nil {
		t.Error("expected Redis delete error")
	}
	if err := client.Ping(context.Background()); err == nil {
		t.Error("expected Redis ping error")
	}
	if err := client.Close(); err != nil {
		t.Errorf("close Redis client: %v", err)
	}
}
