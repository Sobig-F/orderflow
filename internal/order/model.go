package order

import "time"

type Order struct {
	ID        string    `json:"id"`
	Amount    string    `json:"amount"`
	Currency  string    `json:"currency"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CreateOrderRequest struct {
	IdempotencyKey string `json:"-"`
	Amount         string `json:"amount"`
	Currency       string `json:"currency"`
}

type UpdateOrderStatusRequest struct {
	Status string `json:"status"`
}

type OutboxEvent struct {
	ID        int64
	Type      string
	OrderID   string
	Payload   []byte
	CreatedAt time.Time
}

type Page struct {
	Limit  int
	Status string
	Cursor *string
}
