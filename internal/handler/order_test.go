package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Sobig-F/orderflow/internal/order"
	"github.com/Sobig-F/orderflow/internal/service"
)

const testOrderID = "8af426ed-3040-4f05-88ff-0a89846bbcee"

type serviceStub struct {
	createFn func(context.Context, order.CreateOrderRequest) (*order.Order, error)
	getFn    func(context.Context, string) (*order.Order, error)
	listFn   func(context.Context) ([]order.Order, error)
	updateFn func(context.Context, string, string) (*order.Order, error)
}

func (s *serviceStub) CreateOrder(ctx context.Context, req order.CreateOrderRequest) (*order.Order, error) {
	return s.createFn(ctx, req)
}
func (s *serviceStub) GetOrderByID(ctx context.Context, id string) (*order.Order, error) {
	return s.getFn(ctx, id)
}
func (s *serviceStub) ListOrders(ctx context.Context, pages ...order.Page) ([]order.Order, error) {
	return s.listFn(ctx)
}
func (s *serviceStub) UpdateOrderStatus(ctx context.Context, id, status string) (*order.Order, error) {
	return s.updateFn(ctx, id, status)
}

func requestWithID(method, path, id, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.SetPathValue("id", id)
	return req
}

func decodeError(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return body["error"]
}

func TestCreateHandler(t *testing.T) {
	var received order.CreateOrderRequest
	h := &OrderHandler{Service: &serviceStub{createFn: func(_ context.Context, req order.CreateOrderRequest) (*order.Order, error) {
		received = req
		return &order.Order{ID: testOrderID, Amount: req.Amount, Currency: req.Currency}, nil
	}}}
	w := httptest.NewRecorder()
	h.Create(w, httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(`{"amount":"12.50"}`)))
	if w.Code != http.StatusCreated || received.Amount != "12.50" {
		t.Fatalf("status=%d request=%+v body=%s", w.Code, received, w.Body)
	}

	for _, body := range []string{
		`{`,
		`{"amount":"1","unknown":true}`,
		`{"amount":"1"} {"amount":"2"}`,
		`{"amount":"` + strings.Repeat("1", 1<<20) + `"}`,
	} {
		w = httptest.NewRecorder()
		h.Create(w, httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(body)))
		if w.Code != http.StatusBadRequest {
			t.Errorf("body length=%d status=%d, want 400", len(body), w.Code)
		}
	}

	h = &OrderHandler{Service: &serviceStub{createFn: func(context.Context, order.CreateOrderRequest) (*order.Order, error) {
		return nil, fmt.Errorf("%w: bad amount", service.ErrInvalidOrder)
	}}}
	w = httptest.NewRecorder()
	h.Create(w, httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(`{"amount":"0"}`)))
	if w.Code != http.StatusBadRequest || decodeError(t, w) != "bad amount" {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}

	h = &OrderHandler{Service: &serviceStub{createFn: func(context.Context, order.CreateOrderRequest) (*order.Order, error) {
		return nil, errors.New("database failed")
	}}}
	w = httptest.NewRecorder()
	h.Create(w, httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(`{"amount":"1"}`)))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestGetHandler(t *testing.T) {
	h := &OrderHandler{Service: &serviceStub{getFn: func(context.Context, string) (*order.Order, error) { return nil, nil }}}
	w := httptest.NewRecorder()
	h.Get(w, requestWithID(http.MethodGet, "/api/v1/orders/bad", "bad", ""))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid ID status = %d", w.Code)
	}

	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"not found", order.ErrNotFound, http.StatusNotFound},
		{"internal", errors.New("database failed"), http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := &OrderHandler{Service: &serviceStub{getFn: func(context.Context, string) (*order.Order, error) { return nil, test.err }}}
			w := httptest.NewRecorder()
			h.Get(w, requestWithID(http.MethodGet, "/api/v1/orders/"+testOrderID, testOrderID, ""))
			if w.Code != test.want {
				t.Fatalf("status = %d, want %d", w.Code, test.want)
			}
		})
	}

	h = &OrderHandler{Service: &serviceStub{getFn: func(_ context.Context, id string) (*order.Order, error) {
		return &order.Order{ID: id}, nil
	}}}
	w = httptest.NewRecorder()
	h.Get(w, requestWithID(http.MethodGet, "/api/v1/orders/"+testOrderID, testOrderID, ""))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), testOrderID) {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
}

func TestListHandler(t *testing.T) {
	h := &OrderHandler{Service: &serviceStub{listFn: func(context.Context) ([]order.Order, error) { return []order.Order{}, nil }}}
	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequest(http.MethodGet, "/api/v1/orders", nil))
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}

	h = &OrderHandler{Service: &serviceStub{listFn: func(context.Context) ([]order.Order, error) { return nil, errors.New("database failed") }}}
	w = httptest.NewRecorder()
	h.List(w, httptest.NewRequest(http.MethodGet, "/api/v1/orders", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestUpdateStatusHandler(t *testing.T) {
	h := &OrderHandler{Service: &serviceStub{updateFn: func(_ context.Context, id, status string) (*order.Order, error) {
		return &order.Order{ID: id, Status: status}, nil
	}}}
	w := httptest.NewRecorder()
	h.UpdateStatus(w, requestWithID(http.MethodPatch, "/api/v1/orders/"+testOrderID+"/status", testOrderID, `{"status":"processing"}`))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"processing"`) {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}

	invalidID := httptest.NewRecorder()
	h.UpdateStatus(invalidID, requestWithID(http.MethodPatch, "/bad/status", "bad", `{"status":"processing"}`))
	if invalidID.Code != http.StatusBadRequest {
		t.Fatalf("invalid ID status = %d", invalidID.Code)
	}
	invalidBody := httptest.NewRecorder()
	h.UpdateStatus(invalidBody, requestWithID(http.MethodPatch, "/status", testOrderID, `{"status":"processing","extra":true}`))
	if invalidBody.Code != http.StatusBadRequest {
		t.Fatalf("invalid body status = %d", invalidBody.Code)
	}

	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"not found", order.ErrNotFound, http.StatusNotFound},
		{"invalid status", fmt.Errorf("%w: unknown", service.ErrInvalidStatus), http.StatusBadRequest},
		{"invalid transition", fmt.Errorf("%w: terminal", service.ErrInvalidStatusTransition), http.StatusConflict},
		{"conflict", service.ErrStatusConflict, http.StatusConflict},
		{"internal", errors.New("database failed"), http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := &OrderHandler{Service: &serviceStub{updateFn: func(context.Context, string, string) (*order.Order, error) { return nil, test.err }}}
			w := httptest.NewRecorder()
			h.UpdateStatus(w, requestWithID(http.MethodPatch, "/status", testOrderID, `{"status":"processing"}`))
			if w.Code != test.want {
				t.Fatalf("status = %d, want %d", w.Code, test.want)
			}
		})
	}
}

func TestHelperFunctions(t *testing.T) {
	if !validUUID(testOrderID) || validUUID("not-an-id") {
		t.Fatal("UUID validation mismatch")
	}
	if got := messageFor(fmt.Errorf("%w: detail", service.ErrInvalidOrder), service.ErrInvalidOrder); got != "detail" {
		t.Fatalf("messageFor = %q", got)
	}
	if got := messageFor(errors.New("plain error"), service.ErrInvalidOrder); got != "plain error" {
		t.Fatalf("messageFor plain = %q", got)
	}
	w := httptest.NewRecorder()
	writeError(w, http.StatusConflict, "conflict")
	if w.Code != http.StatusConflict || decodeError(t, w) != "conflict" {
		t.Fatalf("writeError status=%d body=%s", w.Code, w.Body)
	}
}

func TestPages(t *testing.T) {
	h := OrderHandler{Service: &serviceStub{listFn: func(context.Context) ([]order.Order, error) { return []order.Order{{ID: testOrderID}, {}}, nil }}}
	for _, q := range []string{"status=x", "limit=0", "cursor=x"} {
		w := httptest.NewRecorder()
		h.List(w, httptest.NewRequest("GET", "/?"+q, nil))
		if w.Code != 400 {
			t.Fatal(q, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.List(w, httptest.NewRequest("GET", "/?status=pending&limit=1&cursor="+testOrderID, nil))
	if w.Code != 200 || w.Header().Get("X-Next-Cursor") != testOrderID {
		t.Fatal(w)
	}
}
