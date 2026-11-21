package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/Sobig-F/orderflow/internal/metrics"
	"regexp"
	"strings"

	"github.com/Sobig-F/orderflow/internal/order"
)

var (
	ErrInvalidOrder            = errors.New("invalid order")
	ErrInvalidStatus           = errors.New("invalid order status")
	ErrInvalidStatusTransition = errors.New("invalid order status transition")
	ErrStatusConflict          = errors.New("order status changed; retry the request")
)

var amountPattern = regexp.MustCompile(`^[0-9]{1,10}(\.[0-9]{1,2})?$`)
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

type OrderRepository interface {
	CreateOrder(context.Context, order.CreateOrderRequest) (*order.Order, error)
	GetOrderByID(context.Context, string) (*order.Order, error)
	ListOrders(context.Context, ...order.Page) ([]order.Order, error)
	UpdateOrderStatus(context.Context, string, string, string) (*order.Order, error)
}

type OrderCache interface {
	GetOrder(context.Context, string) (*order.Order, error)
	SetOrder(context.Context, *order.Order) error
	InvalidateOrder(context.Context, string) error
}

type OrderService struct {
	repository OrderRepository
	cache      OrderCache
}

func NewOrderService(repository OrderRepository, orderCache OrderCache) *OrderService {
	return &OrderService{repository: repository, cache: orderCache}
}

func (s *OrderService) CreateOrder(ctx context.Context, req order.CreateOrderRequest) (*order.Order, error) {
	if !amountPattern.MatchString(req.Amount) || strings.Trim(req.Amount, "0.") == "" {
		return nil, fmt.Errorf("%w: amount must be greater than zero and have at most 2 decimal places", ErrInvalidOrder)
	}

	if req.Currency == "" {
		req.Currency = "RUB"
	}
	if !currencyPattern.MatchString(req.Currency) {
		return nil, fmt.Errorf("%w: currency must be a 3-letter uppercase code", ErrInvalidOrder)
	}

	return s.repository.CreateOrder(ctx, req)
}

func (s *OrderService) GetOrderByID(ctx context.Context, id string) (*order.Order, error) {
	if s.cache != nil {
		if cached, err := s.cache.GetOrder(ctx, id); err == nil {
			metrics.CacheReads.WithLabelValues("hit").Inc()
			return cached, nil
		}
		metrics.CacheReads.WithLabelValues("fallback").Inc()
	}

	result, err := s.repository.GetOrderByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		_ = s.cache.SetOrder(ctx, result)
	}
	return result, nil
}

func (s *OrderService) ListOrders(ctx context.Context, pages ...order.Page) ([]order.Order, error) {
	return s.repository.ListOrders(ctx, pages...)
}

func (s *OrderService) UpdateOrderStatus(ctx context.Context, id, newStatus string) (*order.Order, error) {
	if !isKnownStatus(newStatus) {
		return nil, fmt.Errorf("%w: status must be pending, processing, completed, or failed", ErrInvalidStatus)
	}

	current, err := s.repository.GetOrderByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if !canTransition(current.Status, newStatus) {
		return nil, fmt.Errorf("%w: cannot move from %s to %s", ErrInvalidStatusTransition, current.Status, newStatus)
	}

	updated, err := s.repository.UpdateOrderStatus(ctx, id, current.Status, newStatus)
	if errors.Is(err, order.ErrStatusChanged) {
		return nil, ErrStatusConflict
	}
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		_ = s.cache.InvalidateOrder(ctx, id)
	}

	return updated, nil
}

func isKnownStatus(status string) bool {
	switch status {
	case "pending", "processing", "completed", "failed":
		return true
	default:
		return false
	}
}

func canTransition(current, next string) bool {
	switch current {
	case "pending":
		return next == "processing" || next == "failed"
	case "processing":
		return next == "completed" || next == "failed"
	default:
		return false
	}
}
