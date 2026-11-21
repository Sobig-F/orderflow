package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMiddlewareTracksStatusAndRoute(t *testing.T) {
	metrics := NewHTTPMetrics()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/orders", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	handler := metrics.Middleware(mux)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil))
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusCreated)
	}

	scrape := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := scrape.Body.String()
	if !strings.Contains(body, `orderflow_http_requests_total{method="POST",route="POST /api/v1/orders",status="201"} 1`) {
		t.Fatalf("request counter missing from metrics:\n%s", body)
	}
	if !strings.Contains(body, `orderflow_http_request_duration_seconds_count{method="POST",route="POST /api/v1/orders"} 1`) {
		t.Fatalf("duration observation missing from metrics:\n%s", body)
	}
}

func TestMiddlewareDefaultsStatusAndLabelsUnmatchedRoute(t *testing.T) {
	metrics := NewHTTPMetrics()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(http.ResponseWriter, *http.Request) {})
	handler := metrics.Middleware(mux)

	for _, path := range []string{"/health", "/missing"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}

	scrape := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := scrape.Body.String()
	if !strings.Contains(body, `route="GET /health",status="200"`) {
		t.Fatalf("default 200 metric missing:\n%s", body)
	}
	if !strings.Contains(body, `route="unmatched",status="404"`) {
		t.Fatalf("unmatched route metric missing:\n%s", body)
	}
}

func TestStatusRecorderUnwrap(t *testing.T) {
	base := httptest.NewRecorder()
	recorder := &statusRecorder{ResponseWriter: base, status: http.StatusOK}
	if recorder.Unwrap() != base {
		t.Fatal("Unwrap did not return the underlying response writer")
	}
}
