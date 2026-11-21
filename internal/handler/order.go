package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Sobig-F/orderflow/internal/order"
	"github.com/Sobig-F/orderflow/internal/service"
)

type OrderService interface {
	CreateOrder(context.Context, order.CreateOrderRequest) (*order.Order, error)
	GetOrderByID(context.Context, string) (*order.Order, error)
	ListOrders(context.Context, ...order.Page) ([]order.Order, error)
	UpdateOrderStatus(context.Context, string, string) (*order.Order, error)
}

type OrderHandler struct {
	Service OrderService
}

var uuidPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (h *OrderHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req order.CreateOrderRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	if len(req.IdempotencyKey) > 128 {
		writeError(w, 400, "idempotency key exceeds 128 bytes")
		return
	}
	created, err := h.Service.CreateOrder(ctx, req)
	if err != nil {
		if errors.Is(err, order.ErrKeyConflict) {
			writeError(w, 409, err.Error())
			return
		}
		if errors.Is(err, service.ErrInvalidOrder) {
			writeError(w, http.StatusBadRequest, messageFor(err, service.ErrInvalidOrder))
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create order")
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

func (h *OrderHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validUUID(id) {
		writeError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	result, err := h.Service.GetOrderByID(ctx, id)
	if errors.Is(err, order.ErrNotFound) {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get order")
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *OrderHandler) List(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	q := r.URL.Query()
	page := order.Page{Limit: 20, Status: q.Get("status")}
	switch page.Status {
	case "", "pending", "processing", "completed", "failed":
	default:
		writeError(w, 400, "invalid status")
		return
	}
	if q.Has("limit") {
		n, err := strconv.Atoi(q.Get("limit"))
		if err != nil || n < 1 || n > 100 {
			writeError(w, 400, "limit must be 1 to 100")
			return
		}
		page.Limit = n
	}
	if q.Has("cursor") {
		id := q.Get("cursor")
		if !validUUID(id) {
			writeError(w, 400, "invalid cursor")
			return
		}
		page.Cursor = &id
	}
	limit := page.Limit
	page.Limit++
	results, err := h.Service.ListOrders(ctx, page)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list orders")
		return
	}

	if len(results) > limit {
		w.Header().Set("X-Next-Cursor", results[limit-1].ID)
		results = results[:limit]
	}
	writeJSON(w, http.StatusOK, results)
}

func (h *OrderHandler) UpdateStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validUUID(id) {
		writeError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}

	var req order.UpdateOrderStatusRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	result, err := h.Service.UpdateOrderStatus(ctx, id, req.Status)
	if err != nil {
		switch {
		case errors.Is(err, order.ErrNotFound):
			writeError(w, http.StatusNotFound, "order not found")
		case errors.Is(err, service.ErrInvalidStatus):
			writeError(w, http.StatusBadRequest, messageFor(err, service.ErrInvalidStatus))
		case errors.Is(err, service.ErrInvalidStatusTransition):
			writeError(w, http.StatusConflict, messageFor(err, service.ErrInvalidStatusTransition))
		case errors.Is(err, service.ErrStatusConflict):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "failed to update order status")
		}
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return false
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request must contain a single JSON object")
		return false
	}

	return true
}

func validUUID(id string) bool {
	return uuidPattern.MatchString(id)
}

func messageFor(err, sentinel error) string {
	return strings.TrimPrefix(err.Error(), sentinel.Error()+": ")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
