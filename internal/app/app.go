package app

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/Sobig-F/orderflow/internal/cache"
	"github.com/Sobig-F/orderflow/internal/config"
	"github.com/Sobig-F/orderflow/internal/handler"
	"github.com/Sobig-F/orderflow/internal/messaging"
	"github.com/Sobig-F/orderflow/internal/metrics"
	"github.com/Sobig-F/orderflow/internal/repository"
	"github.com/Sobig-F/orderflow/internal/service"
)

//go:embed openapi.json
var openAPI []byte

type database interface {
	service.OrderRepository
	messaging.OutboxStore
	Ping(context.Context) error
	Close()
}

type orderCache interface {
	service.OrderCache
	Ping(context.Context) error
	Close() error
}

var (
	openDatabase = func(ctx context.Context, url string) (database, error) {
		return repository.NewPostgres(ctx, url)
	}
	openCache = func(ctx context.Context, url string) (orderCache, error) {
		return cache.NewRedis(ctx, url)
	}
	openPublisher = func(brokers, topic string) messaging.Publisher {
		return messaging.NewKafka(brokers, topic)
	}
	listen       = net.Listen
	shutdownHTTP = func(server *http.Server, ctx context.Context) error {
		return server.Shutdown(ctx)
	}
	closeHTTP = func(server *http.Server) error { return server.Close() }
)

func Run(ctx context.Context, cfg config.Config) error {
	dbContext, cancelDatabase := context.WithTimeout(ctx, 10*time.Second)
	db, err := openDatabase(dbContext, cfg.DatabaseURL)
	cancelDatabase()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	cacheContext, cancelCache := context.WithTimeout(ctx, 5*time.Second)
	orderCache, err := openCache(cacheContext, cfg.RedisURL)
	cancelCache()
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	defer func() { _ = orderCache.Close() }()

	publisher := openPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	server := newHTTPServer(cfg.Port, db, orderCache)
	listener, err := listen("tcp", ":"+cfg.Port)
	if err != nil {
		_ = publisher.Close()
		return fmt.Errorf("listen on port %s: %w", cfg.Port, err)
	}
	return serve(ctx, server, listener, db, publisher)
}

func newHTTPServer(port string, db database, orderCache orderCache) *http.Server {
	apiMetrics := metrics.NewHTTPMetrics()
	orderHandler := &handler.OrderHandler{Service: service.NewOrderService(db, orderCache)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(openAPI)
	})
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(`<!doctype html><title>OrderFlow API</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui.css"><div id="api"></div><script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:"/openapi.json",dom_id:"#api"})</script>`))
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "dependency": "postgres"})
			return
		}
		if err := orderCache.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "dependency": "redis"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", apiMetrics.Handler())
	mux.HandleFunc("POST /api/v1/orders", orderHandler.Create)
	mux.HandleFunc("GET /api/v1/orders", orderHandler.List)
	mux.HandleFunc("GET /api/v1/orders/{id}", orderHandler.Get)
	mux.HandleFunc("PATCH /api/v1/orders/{id}/status", orderHandler.UpdateStatus)

	return &http.Server{
		Addr:              ":" + port,
		Handler:           apiMetrics.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, db messaging.OutboxStore, publisher messaging.Publisher) error {
	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		messaging.RunOutboxPublisher(workerCtx, db, publisher)
	}()
	defer func() {
		cancelWorker()
		<-workerDone
		if err := publisher.Close(); err != nil {
			log.Printf("close Kafka publisher: %v", err)
		}
	}()

	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Serve(listener) }()
	select {
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("API server stopped: %w", err)
		}
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdownHTTP(server, shutdownContext); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
			if closeErr := closeHTTP(server); closeErr != nil {
				log.Printf("force close failed: %v", closeErr)
			}
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
