package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sobig-F/orderflow/internal/app"
	"github.com/Sobig-F/orderflow/internal/config"
)

var (
	runApplication = app.Run
	fatal          = log.Fatal
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runApplication(ctx, config.Load()); err != nil {
		fatal(err)
	}
}
