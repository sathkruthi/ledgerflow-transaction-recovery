package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func problem(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]string{"error": message})
}
func operation(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 5*time.Second)
}
func API(s *Store, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	accepted := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ledgerflow_acceptance_total", Help: "Transfer acceptance outcomes."}, []string{"result"})
	latency := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "ledgerflow_acceptance_seconds", Help: "API durable acceptance latency."})
	backlog := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ledgerflow_backlog", Help: "Durable queue and request backlog; valid only when database_up is one."}, []string{"kind"})
	up := prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledgerflow_database_up", Help: "Database availability during last scrape."})
	reg.MustRegister(accepted, latency, backlog, up)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "live"}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, end := context.WithTimeout(r.Context(), time.Second)
		defer end()
		if s.Pool.Ping(ctx) != nil {
			problem(w, 503, "database unavailable")
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx, end := context.WithTimeout(r.Context(), time.Second)
		defer end()
		m, err := s.Backlog(ctx)
		up.Set(0)
		if err == nil {
			up.Set(1)
			for k, v := range m {
				backlog.WithLabelValues(k).Set(float64(v))
			}
		}
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	})
	mux.HandleFunc("POST /transfers", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() { latency.Observe(time.Since(start).Seconds()) }()
		r.Body = http.MaxBytesReader(w, r.Body, MaxPayload)
		b, err := io.ReadAll(r.Body)
		if err != nil {
			accepted.WithLabelValues("invalid").Inc()
			problem(w, 400, "payload exceeds limit")
			return
		}
		request, err := DecodeRequest(b)
		if err != nil {
			accepted.WithLabelValues("invalid").Inc()
			problem(w, 400, err.Error())
			return
		}
		ctx, end := operation(r.Context())
		defer end()
		ctx, span := otel.Tracer("ledgerflow/api").Start(ctx, "transfer.accept")
		defer span.End()
		transfer, replay, err := s.Accept(ctx, r.Header.Get("Idempotency-Key"), request)
		if err != nil {
			if errors.Is(err, ErrConflict) {
				accepted.WithLabelValues("conflict").Inc()
				problem(w, 409, err.Error())
				return
			}
			if errors.Is(err, ErrInvalid) {
				accepted.WithLabelValues("invalid").Inc()
				problem(w, 400, err.Error())
				return
			}
			accepted.WithLabelValues("unavailable").Inc()
			problem(w, 503, "acceptance unavailable; retry with the same idempotency key")
			return
		}
		result := "accepted"
		if replay {
			result = "replayed"
		}
		accepted.WithLabelValues(result).Inc()
		span.SetAttributes(attribute.String("transfer.id", transfer.ID.String()), attribute.String("outcome", result))
		log.Info("transfer accepted", "transfer_id", transfer.ID, "correlation_id", transfer.ID, "replay", replay)
		w.Header().Set("Location", "/transfers/"+transfer.ID.String())
		respond(w, 202, transfer)
	})
	mux.HandleFunc("GET /transfers/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			problem(w, 400, "invalid transfer ID")
			return
		}
		ctx, end := operation(r.Context())
		defer end()
		t, err := s.Get(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			problem(w, 404, "transfer not found")
			return
		}
		if err != nil {
			problem(w, 503, "database unavailable")
			return
		}
		respond(w, 200, t)
	})
	mux.HandleFunc("GET /accounts", func(w http.ResponseWriter, r *http.Request) {
		ctx, end := operation(r.Context())
		defer end()
		a, err := s.Accounts(ctx)
		if err != nil {
			problem(w, 503, "database unavailable")
			return
		}
		respond(w, 200, a)
	})
	mux.HandleFunc("GET /reconcile", func(w http.ResponseWriter, r *http.Request) {
		ctx, end := operation(r.Context())
		defer end()
		report, err := s.Reconcile(ctx)
		if err != nil {
			problem(w, 503, "reconciliation unavailable")
			return
		}
		status := 200
		if !report.Balanced {
			status = 409
		}
		respond(w, status, report)
	})
	for _, item := range []struct{ path, query string }{{"/journals", `SELECT COALESCE(jsonb_agg(row_to_json(j)),'[]'::jsonb) FROM (SELECT j.id,j.transfer_id,j.created_at,(SELECT jsonb_agg(jsonb_build_object('account',e.account_id,'amount_minor',e.amount) ORDER BY e.id) FROM entries e WHERE e.journal_id=j.id) AS entries FROM journals j ORDER BY j.created_at DESC,j.id LIMIT 100) j`}, {"/audit", `SELECT COALESCE(jsonb_agg(row_to_json(a)),'[]'::jsonb) FROM (SELECT id,transfer_id,kind,details,created_at FROM audit ORDER BY id DESC LIMIT 100) a`}, {"/queue", `SELECT COALESCE(jsonb_agg(row_to_json(d)),'[]'::jsonb) FROM (SELECT id,event_id,state,attempts,last_error FROM deliveries ORDER BY id DESC LIMIT 100) d`}} {
		mux.HandleFunc("GET "+item.path, func(w http.ResponseWriter, r *http.Request) {
			ctx, end := operation(r.Context())
			defer end()
			var raw json.RawMessage
			if err := s.Pool.QueryRow(ctx, item.query).Scan(&raw); err != nil {
				problem(w, 503, "database unavailable")
				return
			}
			respond(w, 200, raw)
		})
	}
	return mux
}

func Relay(s *Store, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, end := context.WithTimeout(r.Context(), time.Second)
		defer end()
		if s.Pool.Ping(ctx) != nil {
			problem(w, 503, "database unavailable")
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, MaxPayload)
		b, err := io.ReadAll(r.Body)
		if err != nil {
			problem(w, 400, "payload exceeds limit")
			return
		}
		e, err := DecodeEvent(b)
		if err != nil {
			problem(w, 400, "invalid event contract")
			return
		}
		parent := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, end := operation(parent)
		defer end()
		ctx, span := otel.Tracer("ledgerflow/relay").Start(ctx, "relay.append")
		defer span.End()
		span.SetAttributes(attribute.String("event.id", e.ID.String()))
		id, err := s.Append(ctx, e)
		if err != nil {
			problem(w, 503, "durable relay unavailable")
			return
		}
		log.Info("event durably received", "event_id", e.ID, "delivery_id", id, "correlation_id", e.ID)
		respond(w, 201, map[string]int64{"delivery_id": id})
	})
	return mux
}
