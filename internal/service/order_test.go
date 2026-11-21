package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Sobig-F/orderflow/internal/order"
)

type repositoryStub struct {
	createFn func(context.Context, order.CreateOrderRequest) (*order.Order, error)
	getFn    func(context.Context, string) (*order.Order, error)
	listFn   func(context.Context) ([]order.Order, error)
	updateFn func(context.Context, string, string, string) (*order.Order, error)
}

func (r *repositoryStub) CreateOrder(ctx context.Context, req order.CreateOrderRequest) (*order.Order, error) {
	return r.createFn(ctx, req)
}
func (r *repositoryStub) GetOrderByID(ctx context.Context, id string) (*order.Order, error) {
	return r.getFn(ctx, id)
}
func (r *repositoryStub) ListOrders(ctx context.Context, pages ...order.Page) ([]order.Order, error) {
	return r.listFn(ctx)
}
func (r *repositoryStub) UpdateOrderStatus(ctx context.Context, id, oldStatus, newStatus string) (*order.Order, error) {
	return r.updateFn(ctx, id, oldStatus, newStatus)
}

type cacheStub struct {
	value         *order.Order
	getErr        error
	setErr        error
	invalidateErr error
	setCalls      int
	deleteCalls   int
}

func (c *cacheStub) GetOrder(context.Context, string) (*order.Order, error) {
	return c.value, c.getErr
}
func (c *cacheStub) SetOrder(context.Context, *order.Order) error {
	c.setCalls++
	return c.setErr
}
func (c *cacheStub) InvalidateOrder(context.Context, string) error {
	c.deleteCalls++
	return c.invalidateErr
}

func TestCreateOrderValidationAndDefaults(t *testing.T) {
	var received order.CreateOrderRequest
	repo := &repositoryStub{createFn: func(_ context.Context, req order.CreateOrderRequest) (*order.Order, error) {
		received = req
		return &order.Order{Amount: req.Amount, Currency: req.Currency}, nil
	}}
	svc := NewOrderService(repo, nil)
	created, err := svc.CreateOrder(context.Background(), order.CreateOrderRequest{Amount: "250.00"})
	if err != nil {
		t.Fatal(err)
	}
	if received.Currency != "RUB" || created.Currency != "RUB" {
		t.Fatalf("currency = %q, want RUB", created.Currency)
	}

	for _, amount := range []string{"", "0", "0.00", "12.345", "10000000000"} {
		_, err := svc.CreateOrder(context.Background(), order.CreateOrderRequest{Amount: amount})
		if !errors.Is(err, ErrInvalidOrder) {
			t.Errorf("amount %q: expected ErrInvalidOrder, got %v", amount, err)
		}
	}
	_, err = svc.CreateOrder(context.Background(), order.CreateOrderRequest{Amount: "1", Currency: "usd"})
	if !errors.Is(err, ErrInvalidOrder) {
		t.Fatalf("lowercase currency: expected ErrInvalidOrder, got %v", err)
	}
}

func TestCreateOrderRepositoryError(t *testing.T) {
	wantErr := errors.New("database unavailable")
	svc := NewOrderService(&repositoryStub{createFn: func(context.Context, order.CreateOrderRequest) (*order.Order, error) {
		return nil, wantErr
	}}, nil)
	_, err := svc.CreateOrder(context.Background(), order.CreateOrderRequest{Amount: "1", Currency: "USD"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestGetOrderCachePaths(t *testing.T) {
	want := &order.Order{ID: "cached"}
	cache := &cacheStub{value: want}
	called := false
	svc := NewOrderService(&repositoryStub{getFn: func(context.Context, string) (*order.Order, error) {
		called = true
		return nil, nil
	}}, cache)
	got, err := svc.GetOrderByID(context.Background(), "cached")
	if err != nil || got != want || called {
		t.Fatalf("cache hit got=%v err=%v repositoryCalled=%v", got, err, called)
	}

	cache = &cacheStub{getErr: errors.New("cache unavailable"), setErr: errors.New("write unavailable")}
	dbOrder := &order.Order{ID: "from-db"}
	svc = NewOrderService(&repositoryStub{getFn: func(context.Context, string) (*order.Order, error) {
		return dbOrder, nil
	}}, cache)
	got, err = svc.GetOrderByID(context.Background(), "from-db")
	if err != nil || got != dbOrder || cache.setCalls != 1 {
		t.Fatalf("fallback got=%v err=%v cacheSetCalls=%d", got, err, cache.setCalls)
	}

	wantErr := order.ErrNotFound
	svc = NewOrderService(&repositoryStub{getFn: func(context.Context, string) (*order.Order, error) {
		return nil, wantErr
	}}, &cacheStub{getErr: errors.New("miss")})
	if _, err := svc.GetOrderByID(context.Background(), "missing"); !errors.Is(err, wantErr) {
		t.Fatalf("repository error = %v, want %v", err, wantErr)
	}
}

func TestListOrders(t *testing.T) {
	want := []order.Order{{ID: "one"}}
	svc := NewOrderService(&repositoryStub{listFn: func(context.Context) ([]order.Order, error) { return want, nil }}, nil)
	got, err := svc.ListOrders(context.Background())
	if err != nil || len(got) != 1 || got[0].ID != "one" {
		t.Fatalf("got=%v err=%v", got, err)
	}
	wantErr := errors.New("query failed")
	svc = NewOrderService(&repositoryStub{listFn: func(context.Context) ([]order.Order, error) { return nil, wantErr }}, nil)
	if _, err := svc.ListOrders(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestUpdateOrderStatusTransitions(t *testing.T) {
	for _, test := range []struct {
		current string
		next    string
	}{
		{"pending", "processing"},
		{"pending", "failed"},
		{"processing", "completed"},
		{"processing", "failed"},
	} {
		t.Run(test.current+"-to-"+test.next, func(t *testing.T) {
			cache := &cacheStub{}
			repo := &repositoryStub{
				getFn: func(context.Context, string) (*order.Order, error) {
					return &order.Order{ID: "id", Status: test.current}, nil
				},
				updateFn: func(_ context.Context, _, old, next string) (*order.Order, error) {
					if old != test.current || next != test.next {
						t.Fatalf("update got %s -> %s", old, next)
					}
					return &order.Order{ID: "id", Status: next}, nil
				},
			}
			got, err := NewOrderService(repo, cache).UpdateOrderStatus(context.Background(), "id", test.next)
			if err != nil || got.Status != test.next || cache.deleteCalls != 1 {
				t.Fatalf("got=%v err=%v invalidations=%d", got, err, cache.deleteCalls)
			}
		})
	}
}

func TestUpdateOrderStatusErrors(t *testing.T) {
	baseRepo := func(current string, getErr, updateErr error) *repositoryStub {
		return &repositoryStub{
			getFn: func(context.Context, string) (*order.Order, error) {
				if getErr != nil {
					return nil, getErr
				}
				return &order.Order{Status: current}, nil
			},
			updateFn: func(context.Context, string, string, string) (*order.Order, error) { return nil, updateErr },
		}
	}

	if _, err := NewOrderService(baseRepo("pending", nil, nil), nil).UpdateOrderStatus(context.Background(), "id", "shipped"); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("unknown status error = %v", err)
	}
	if _, err := NewOrderService(baseRepo("completed", nil, nil), nil).UpdateOrderStatus(context.Background(), "id", "processing"); !errors.Is(err, ErrInvalidStatusTransition) {
		t.Fatalf("terminal transition error = %v", err)
	}
	if _, err := NewOrderService(baseRepo("pending", order.ErrNotFound, nil), nil).UpdateOrderStatus(context.Background(), "id", "processing"); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("not found error = %v", err)
	}
	if _, err := NewOrderService(baseRepo("pending", nil, order.ErrStatusChanged), nil).UpdateOrderStatus(context.Background(), "id", "processing"); !errors.Is(err, ErrStatusConflict) {
		t.Fatalf("concurrent status error = %v", err)
	}
	wantErr := errors.New("update failed")
	if _, err := NewOrderService(baseRepo("pending", nil, wantErr), nil).UpdateOrderStatus(context.Background(), "id", "processing"); !errors.Is(err, wantErr) {
		t.Fatalf("update error = %v", err)
	}
}
