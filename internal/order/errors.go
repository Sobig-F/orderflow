package order

import "errors"

var ErrKeyConflict = errors.New("idempotency key used with different parameters")

var (
	ErrNotFound      = errors.New("order not found")
	ErrStatusChanged = errors.New("order status changed concurrently")
)
