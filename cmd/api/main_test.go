package main

import (
	"context"
	"errors"
	"testing"

	"github.com/Sobig-F/orderflow/internal/config"
)

func TestMainDelegatesAndHandlesErrors(t *testing.T) {
	oldRun, oldFatal := runApplication, fatal
	t.Cleanup(func() { runApplication, fatal = oldRun, oldFatal })

	called := false
	runApplication = func(ctx context.Context, cfg config.Config) error {
		called = true
		if ctx == nil || cfg.Port != "8080" {
			t.Fatalf("unexpected context or config: ctx=%v config=%+v", ctx, cfg)
		}
		return nil
	}
	fatal = func(...any) { t.Fatal("fatal called for successful run") }
	main()
	if !called {
		t.Fatal("application was not called")
	}

	wantErr := errors.New("startup error")
	runApplication = func(context.Context, config.Config) error { return wantErr }
	var fatalCalled bool
	fatal = func(args ...any) {
		fatalCalled = true
		if len(args) != 1 || !errors.Is(args[0].(error), wantErr) {
			t.Errorf("fatal arguments = %v", args)
		}
	}
	main()
	if !fatalCalled {
		t.Fatal("fatal was not called for an application error")
	}
}
