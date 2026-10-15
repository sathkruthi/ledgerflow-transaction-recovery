package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sathkruthi/ledgerflow-transaction-recovery/internal/ledger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	trace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("ledgerflow stopped", "reason", err.Error())
		os.Exit(1)
	}
}
func run(log *slog.Logger) error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: ledgerflow init|api|relay|publisher|consumer|retry DELIVERY_ID")
	}
	mode := os.Args[1]
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return fmt.Errorf("trace exporter configuration failed")
		}
		tp := trace.NewTracerProvider(trace.WithBatcher(exp), trace.WithResource(resource.NewWithAttributes("", attribute.String("service.name", "ledgerflow-"+mode))))
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.TraceContext{})
		defer func() {
			end, done := context.WithTimeout(context.Background(), 3*time.Second)
			defer done()
			tp.Shutdown(end)
		}()
	}
	s, err := ledger.Open(ctx, url)
	if err != nil {
		return err
	}
	defer s.Pool.Close()
	switch mode {
	case "init":
		op, end := context.WithTimeout(ctx, 15*time.Second)
		defer end()
		if err = s.Migrate(op); err != nil {
			return err
		}
		return s.Seed(op, 100000)
	case "retry":
		if len(os.Args) != 3 {
			return fmt.Errorf("retry requires a delivery ID")
		}
		id, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil || id < 1 {
			return fmt.Errorf("invalid delivery ID")
		}
		op, end := context.WithTimeout(ctx, 5*time.Second)
		defer end()
		return s.RetryDead(op, id)
	case "publisher", "consumer":
		relay := os.Getenv("RELAY_URL")
		if relay == "" {
			relay = "http://relay:8091"
		}
		var crash func()
		if os.Getenv("LOCAL_CRASH_AFTER_PUBLISH") == "1" {
			if os.Getenv("ENABLE_LOCAL_FAULTS") != "1" {
				return fmt.Errorf("local fault injection requires ENABLE_LOCAL_FAULTS=1")
			}
			crash = func() { log.Warn("intentional local publisher crash after relay acknowledgement"); os.Exit(77) }
		}
		s.RunWorker(ctx, mode, relay, 10*time.Second, crash, log)
		return nil
	case "api", "relay":
		address := os.Getenv("LISTEN_ADDR")
		if address == "" {
			address = ":8090"
			if mode == "relay" {
				address = ":8091"
			}
		}
		handler := ledger.API(s, log)
		if mode == "relay" {
			handler = ledger.Relay(s, log)
		}
		server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
		errs := make(chan error, 1)
		go func() { errs <- server.ListenAndServe() }()
		log.Info("service started", "mode", mode)
		select {
		case <-ctx.Done():
		case err = <-errs:
			cancel()
			if err != http.ErrServerClosed {
				return err
			}
		}
		end, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		return server.Shutdown(end)
	default:
		return fmt.Errorf("unknown mode")
	}
}
