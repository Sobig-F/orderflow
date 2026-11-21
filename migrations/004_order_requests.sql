CREATE TABLE IF NOT EXISTS order_requests (key text PRIMARY KEY, amount numeric(12,2) NOT NULL, currency text NOT NULL, response jsonb NOT NULL);
