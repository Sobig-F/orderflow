package app

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Sobig-F/orderflow/internal/config"
	"github.com/Sobig-F/orderflow/internal/messaging"
	"github.com/Sobig-F/orderflow/internal/order"
)

type databaseStub struct {
	pingErr error
	closed  bool
	list    []order.Order
}

func (d *databaseStub) CreateOrder(context.Context, order.CreateOrderRequest) (*order.Order, error) {
	return &order.Order{ID: "id"}, nil
}
func (d *databaseStub) GetOrderByID(context.Context, string) (*order.Order, error) {
	return nil, order.ErrNotFound
}
func (d *databaseStub) ListOrders(context.Context, ...order.Page) ([]order.Order, error) {
	return d.list, nil
}
func (d *databaseStub) UpdateOrderStatus(context.Context, string, string, string) (*order.Order, error) {
	return &order.Order{ID: "id", Status: "processing"}, nil
}
func (d *databaseStub) PendingOutboxEvents(context.Context, int) ([]order.OutboxEvent, error) {
	return nil, nil
}
func (*databaseStub) MarkOutboxEventPublished(context.Context, int64) error    { return nil }
func (*databaseStub) RecordOutboxFailure(context.Context, int64, string) error { return nil }
func (d *databaseStub) Ping(context.Context) error                             { return d.pingErr }
func (d *databaseStub) Close()                                                 { d.closed = true }

type cacheStub struct {
	pingErr error
	closed  bool
}

func (*cacheStub) GetOrder(context.Context, string) (*order.Order, error) {
	return nil, errors.New("cache miss")
}
func (*cacheStub) SetOrder(context.Context, *order.Order) error  { return nil }
func (*cacheStub) InvalidateOrder(context.Context, string) error { return nil }
func (c *cacheStub) Ping(context.Context) error                  { return c.pingErr }
func (c *cacheStub) Close() error                                { c.closed = true; return nil }

type publisherStub struct{ closed bool }

func (*publisherStub) Publish(context.Context, order.OutboxEvent) error { return nil }
func (p *publisherStub) Close() error                                   { p.closed = true; return nil }

func setFactories(t *testing.T) {
	t.Helper()
	oldDatabase, oldCache, oldPublisher, oldListen := openDatabase, openCache, openPublisher, listen
	oldShutdown, oldClose := shutdownHTTP, closeHTTP
	t.Cleanup(func() {
		openDatabase, openCache, openPublisher, listen = oldDatabase, oldCache, oldPublisher, oldListen
		shutdownHTTP, closeHTTP = oldShutdown, oldClose
	})
}

func TestRunServesAPIAndReadiness(t *testing.T) {
	setFactories(t)
	db := &databaseStub{list: []order.Order{}}
	redis := &cacheStub{}
	publisher := &publisherStub{}
	listenerChannel := make(chan net.Listener, 1)
	openDatabase = func(context.Context, string) (database, error) { return db, nil }
	openCache = func(context.Context, string) (orderCache, error) { return redis, nil }
	openPublisher = func(string, string) messaging.Publisher { return publisher }
	listen = func(string, string) (net.Listener, error) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err == nil {
			listenerChannel <- listener
		}
		return listener, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, config.Config{DatabaseURL: "db", RedisURL: "redis", KafkaTopic: "events", Port: "8080"})
	}()
	listener := <-listenerChannel
	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: time.Second}
	get := func(path string) (*http.Response, string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			response, err := client.Get(baseURL + path)
			if err == nil {
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if readErr != nil {
					t.Fatal(readErr)
				}
				return response, string(body)
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("API did not start for %s", path)
		return nil, ""
	}

	response, _ := get("/health")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", response.StatusCode)
	}
	response, _ = get("/ready")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ready status = %d", response.StatusCode)
	}
	db.pingErr = errors.New("postgres ping failed")
	response, body := get("/ready")
	if response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "postgres") {
		t.Fatalf("database readiness status=%d body=%s", response.StatusCode, body)
	}
	db.pingErr = nil
	redis.pingErr = errors.New("redis ping failed")
	response, body = get("/ready")
	if response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "redis") {
		t.Fatalf("Redis readiness status=%d body=%s", response.StatusCode, body)
	}
	redis.pingErr = nil
	response, body = get("/metrics")
	if response.StatusCode != http.StatusOK || !strings.Contains(body, "orderflow_http_requests_total") {
		t.Fatalf("metrics status=%d body=%s", response.StatusCode, body)
	}
	response, body = get("/api/v1/orders")
	if response.StatusCode != http.StatusOK || strings.TrimSpace(body) != "[]" {
		t.Fatalf("list status=%d body=%s", response.StatusCode, body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("API did not shut down")
	}
	if !db.closed || !redis.closed || !publisher.closed {
		t.Fatalf("resources not closed: db=%v redis=%v publisher=%v", db.closed, redis.closed, publisher.closed)
	}
}

func TestRunStartupErrors(t *testing.T) {
	setFactories(t)
	dbErr := errors.New("postgres down")
	openDatabase = func(context.Context, string) (database, error) { return nil, dbErr }
	if err := Run(context.Background(), config.Config{}); !errors.Is(err, dbErr) {
		t.Fatalf("database startup error = %v", err)
	}

	db := &databaseStub{}
	cacheErr := errors.New("redis down")
	openDatabase = func(context.Context, string) (database, error) { return db, nil }
	openCache = func(context.Context, string) (orderCache, error) { return nil, cacheErr }
	if err := Run(context.Background(), config.Config{}); !errors.Is(err, cacheErr) || !db.closed {
		t.Fatalf("cache startup error=%v database closed=%v", err, db.closed)
	}

	redis := &cacheStub{}
	publisher := &publisherStub{}
	openCache = func(context.Context, string) (orderCache, error) { return redis, nil }
	openPublisher = func(string, string) messaging.Publisher { return publisher }
	listen = func(string, string) (net.Listener, error) { return nil, errors.New("port in use") }
	db = &databaseStub{}
	openDatabase = func(context.Context, string) (database, error) { return db, nil }
	if err := Run(context.Background(), config.Config{Port: "8080"}); err == nil || !publisher.closed || !db.closed || !redis.closed {
		t.Fatalf("listen failure=%v publisher=%v db=%v redis=%v", err, publisher.closed, db.closed, redis.closed)
	}
}

func TestServeReturnsListenerErrors(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	db := &databaseStub{}
	publisher := &publisherStub{}
	server := newHTTPServer("8080", db, &cacheStub{})
	if err := serve(context.Background(), server, listener, db, publisher); err == nil {
		t.Fatal("expected listener error")
	}
	if !publisher.closed {
		t.Fatal("publisher was not closed after listener error")
	}
}

func TestServeClosedServerAndShutdownFallback(t *testing.T) {
	setFactories(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := newHTTPServer("8080", &databaseStub{}, &cacheStub{})
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := serve(context.Background(), server, listener, &databaseStub{}, &publisherStub{}); err != nil {
		t.Fatalf("closed server returned error: %v", err)
	}
	_ = listener.Close()

	listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	closeErr := errors.New("force close failed")
	shutdownHTTP = func(*http.Server, context.Context) error { return errors.New("shutdown timed out") }
	closeHTTP = func(server *http.Server) error {
		_ = server.Close()
		return closeErr
	}
	server = newHTTPServer("8080", &databaseStub{}, &cacheStub{})
	if err := serve(ctx, server, listener, &databaseStub{}, &publisherStub{}); err != nil {
		t.Fatalf("shutdown fallback returned error: %v", err)
	}
	_ = listener.Close()
}

func TestDefaultFactories(t *testing.T) {
	if _, err := openDatabase(context.Background(), "not a database URL"); err == nil {
		t.Fatal("expected invalid PostgreSQL URL")
	}
	if _, err := openCache(context.Background(), "not a Redis URL"); err == nil {
		t.Fatal("expected invalid Redis URL")
	}
	publisher := openPublisher("localhost:9092", "test-topic")
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}
