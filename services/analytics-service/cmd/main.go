package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type state struct {
	db        *pgxpool.Pool
	redis     *redis.Client
	reader    *kafka.Reader
	logger    *zap.Logger
	secret    []byte
	service   string
	requests  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	published *prometheus.CounterVec
	consumed  *prometheus.CounterVec
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type event struct {
	EventType  string         `json:"event_type"`
	Payload    map[string]any `json:"payload"`
	OccurredAt string         `json:"occurred_at"`
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	serviceName := env("SERVICE_NAME", "analytics-service")
	cfg, err := pgxpool.ParseConfig(env("DATABASE_URL", "postgres://profitlens:profitlens@localhost:5432/profitlens?sslmode=disable"))
	if err != nil {
		logger.Fatal("failed to parse database url", zap.Error(err))
	}
	cfg.MaxConns = 25
	db, err := pgxpool.NewWithConfig(context.TODO(), cfg)
	if err != nil {
		logger.Fatal("failed to connect database", zap.Error(err))
	}
	defer db.Close()
	s := &state{
		db:        db,
		redis:     redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")}),
		reader:    kafka.NewReader(kafka.ReaderConfig{Brokers: strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","), Topic: env("KAFKA_TOPIC", "profitlens.events"), GroupID: env("KAFKA_GROUP_ID", "analytics-service")}),
		logger:    logger,
		secret:    []byte(env("JWT_SECRET", "change-me")),
		service:   serviceName,
		requests:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "Total HTTP requests.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"method", "path", "status"}),
		duration:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request duration.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"method", "path"}),
		published: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kafka_events_published_total", Help: "Published Kafka events.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"event_type"}),
		consumed:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kafka_events_consumed_total", Help: "Consumed Kafka events.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"event_type"}),
	}
	prometheus.MustRegister(s.requests, s.duration, s.published, s.consumed)
	consumerCtx, cancelConsumer := context.WithCancel(context.TODO())
	go s.consume(consumerCtx)

	app := fiber.New(fiber.Config{ErrorHandler: s.errorHandler})
	app.Use(requestid.New(), recover.New(), cors.New(), s.metrics(), s.auth(), s.rateLimit())
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
	app.Get("/dashboard", s.dashboard)
	app.Get("/reports/profitability", s.profitabilityReport)
	app.Get("/reports/clients", s.clientsReport)
	app.Get("/reports/best-projects", s.bestProjects)
	app.Get("/reports/worst-clients", s.worstClients)
	go func() {
		if err := app.Listen(":" + env("PORT", "8004")); err != nil {
			logger.Fatal("server stopped", zap.Error(err))
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancelConsumer()
	_ = app.ShutdownWithTimeout(10 * time.Second)
	_ = s.reader.Close()
	_ = s.redis.Close()
}

func (s *state) consume(ctx context.Context) {
	for {
		msg, err := s.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Error("failed to read kafka message", zap.Error(err))
			continue
		}
		var evt event
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			s.logger.Error("failed to decode kafka event", zap.Error(err))
			continue
		}
		s.consumed.WithLabelValues(evt.EventType).Inc()
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		projectID, _ := evt.Payload["project_id"].(string)
		switch evt.EventType {
		case "time.entry.created", "expense.added", "invoice.paid", "project.closed":
			if projectID != "" {
				_ = s.calculateProject(requestCtx, projectID)
			}
			if evt.EventType == "invoice.paid" && projectID != "" {
				_ = s.recalculateClientRisk(requestCtx, projectID)
			}
		case "invoice.overdue":
			if projectID != "" {
				_ = s.increaseRisk(requestCtx, projectID)
			}
		}
		cancel()
	}
}

func (s *state) calculateProject(ctx context.Context, projectID string) error {
	_, err := s.db.Exec(ctx, `
		WITH totals AS (
			SELECT p.id AS project_id,
				COALESCE((SELECT SUM(amount_cents) FROM billing.invoices WHERE project_id=p.id AND status='paid' AND deleted_at IS NULL),0)::bigint AS revenue_cents,
				COALESCE((SELECT SUM((te.hours * u.hourly_cost * 100)::bigint) FROM project.time_entries te JOIN auth.users u ON u.id=te.user_id WHERE te.project_id=p.id AND te.deleted_at IS NULL),0)::bigint
				+ COALESCE((SELECT SUM(amount_cents) FROM project.expenses WHERE project_id=p.id AND deleted_at IS NULL),0)::bigint AS cost_cents
			FROM project.projects p WHERE p.id=$1 AND p.deleted_at IS NULL
		)
		INSERT INTO analytics.profit_snapshots (project_id,revenue_cents,cost_cents,net_profit_cents,margin_percent)
		SELECT project_id,revenue_cents,cost_cents,revenue_cents-cost_cents,
			CASE WHEN revenue_cents=0 THEN 0 ELSE ROUND(((revenue_cents-cost_cents)::numeric / revenue_cents::numeric) * 100, 2) END
		FROM totals`, projectID)
	if err == nil {
		_ = s.redis.Del(ctx, "profit:project:"+projectID).Err()
	}
	return err
}

func (s *state) recalculateClientRisk(ctx context.Context, projectID string) error {
	_, err := s.db.Exec(ctx, `
		WITH project_client AS (
			SELECT c.id AS client_id FROM project.projects p JOIN project.clients c ON c.id=p.client_id WHERE p.id=$1
		), stats AS (
			SELECT pc.client_id,
				LEAST(30, COUNT(*) FILTER (WHERE i.status='overdue') * 10)::int AS overdue_score,
				LEAST(25, COALESCE(AVG(GREATEST(EXTRACT(DAY FROM (COALESCE(i.paid_at, now()) - i.due_date::timestamp)), 0)),0)::int) AS delay_score,
				CASE WHEN COALESCE(SUM(i.amount_cents),0) < 100000 THEN 10 ELSE 0 END AS volume_score,
				CASE WHEN COALESCE(AVG(ps.margin_percent),0) < 10 THEN 15 ELSE 0 END AS margin_score
			FROM project_client pc
			LEFT JOIN project.projects p ON p.client_id=pc.client_id
			LEFT JOIN billing.invoices i ON i.project_id=p.id AND i.deleted_at IS NULL
			LEFT JOIN LATERAL (SELECT margin_percent FROM analytics.profit_snapshots WHERE project_id=p.id ORDER BY calculated_at DESC LIMIT 1) ps ON true
			GROUP BY pc.client_id
		)
		UPDATE project.clients c SET risk_score=LEAST(100, s.overdue_score+s.delay_score+s.volume_score+s.margin_score), updated_at=now()
		FROM stats s WHERE c.id=s.client_id`, projectID)
	return err
}

func (s *state) increaseRisk(ctx context.Context, projectID string) error {
	_, err := s.db.Exec(ctx, `UPDATE project.clients c SET risk_score=LEAST(100, risk_score+10), updated_at=now() FROM project.projects p WHERE p.client_id=c.id AND p.id=$1`, projectID)
	return err
}

func (s *state) dashboard(c *fiber.Ctx) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	key := "dashboard:user:" + userID(c)
	if cached, err := s.redis.Get(ctx, key).Result(); err == nil {
		var data fiber.Map
		_ = json.Unmarshal([]byte(cached), &data)
		return s.ok(c, data)
	}
	var activeProjects, overdueInvoices int
	var revenue, netProfit int64
	err := s.db.QueryRow(ctx, `SELECT COUNT(*) FILTER (WHERE p.status='active'), COALESCE(SUM(i.amount_cents) FILTER (WHERE i.status='paid'),0), COUNT(i.*) FILTER (WHERE i.status='overdue') FROM project.clients c LEFT JOIN project.projects p ON p.client_id=c.id AND p.deleted_at IS NULL LEFT JOIN billing.invoices i ON i.project_id=p.id AND i.deleted_at IS NULL WHERE c.user_id=$1 AND c.deleted_at IS NULL`, userID(c)).Scan(&activeProjects, &revenue, &overdueInvoices)
	if err != nil {
		return err
	}
	_ = s.db.QueryRow(ctx, `SELECT COALESCE(SUM(latest.net_profit_cents),0) FROM project.clients c JOIN project.projects p ON p.client_id=c.id LEFT JOIN LATERAL (SELECT net_profit_cents FROM analytics.profit_snapshots WHERE project_id=p.id ORDER BY calculated_at DESC LIMIT 1) latest ON true WHERE c.user_id=$1`, userID(c)).Scan(&netProfit)
	data := fiber.Map{"active_projects": activeProjects, "revenue_cents": revenue, "net_profit_cents": netProfit, "overdue_invoices": overdueInvoices}
	raw, _ := json.Marshal(data)
	_ = s.redis.Set(ctx, key, raw, 5*time.Minute).Err()
	return s.ok(c, data)
}

func (s *state) profitabilityReport(c *fiber.Ctx) error {
	period := c.Query("period", "monthly")
	trunc := map[string]string{"weekly": "week", "monthly": "month", "yearly": "year"}[period]
	if trunc == "" {
		return s.fail(c, 400, "INVALID_PERIOD", "Period must be weekly, monthly, or yearly")
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	rows, err := s.db.Query(ctx, `SELECT date_trunc($1, ps.calculated_at)::date AS period, SUM(ps.revenue_cents), SUM(ps.cost_cents), SUM(ps.net_profit_cents) FROM analytics.profit_snapshots ps JOIN project.projects p ON p.id=ps.project_id JOIN project.clients c ON c.id=p.client_id WHERE c.user_id=$2 GROUP BY 1 ORDER BY 1 DESC`, trunc, userID(c))
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []fiber.Map{}
	for rows.Next() {
		var p time.Time
		var revenue, cost, profit int64
		if err := rows.Scan(&p, &revenue, &cost, &profit); err != nil {
			return err
		}
		items = append(items, fiber.Map{"period": p.Format("2006-01-02"), "revenue_cents": revenue, "cost_cents": cost, "net_profit_cents": profit})
	}
	return s.ok(c, items)
}

func (s *state) clientsReport(c *fiber.Ctx) error {
	return s.simpleRows(c, `SELECT cl.id,cl.name,cl.risk_score,COALESCE(SUM(i.amount_cents) FILTER (WHERE i.status='paid'),0) FROM project.clients cl LEFT JOIN project.projects p ON p.client_id=cl.id LEFT JOIN billing.invoices i ON i.project_id=p.id WHERE cl.user_id=$1 AND cl.deleted_at IS NULL GROUP BY cl.id,cl.name,cl.risk_score ORDER BY 4 DESC`, []string{"id", "name", "risk_score", "revenue_cents"})
}
func (s *state) bestProjects(c *fiber.Ctx) error {
	return s.simpleRows(c, `SELECT p.id,p.name,COALESCE(latest.net_profit_cents,0),COALESCE(latest.margin_percent::text,'0.00') FROM project.projects p JOIN project.clients c ON c.id=p.client_id LEFT JOIN LATERAL (SELECT net_profit_cents,margin_percent FROM analytics.profit_snapshots WHERE project_id=p.id ORDER BY calculated_at DESC LIMIT 1) latest ON true WHERE c.user_id=$1 AND p.deleted_at IS NULL ORDER BY COALESCE(latest.net_profit_cents,0) DESC LIMIT 5`, []string{"id", "name", "net_profit_cents", "margin_percent"})
}
func (s *state) worstClients(c *fiber.Ctx) error {
	return s.simpleRows(c, `SELECT id,name,risk_score,currency FROM project.clients WHERE user_id=$1 AND deleted_at IS NULL ORDER BY risk_score DESC LIMIT 5`, []string{"id", "name", "risk_score", "currency"})
}

func (s *state) simpleRows(c *fiber.Ctx, query string, columns []string) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	rows, err := s.db.Query(ctx, query, userID(c))
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []fiber.Map{}
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		item := fiber.Map{}
		for i, col := range columns {
			item[col] = values[i]
		}
		items = append(items, item)
	}
	return s.ok(c, items)
}

func (s *state) auth() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if c.Path() == "/metrics" {
			return c.Next()
		}
		token := strings.TrimPrefix(c.Get("Authorization"), "Bearer ")
		claims := jwt.MapClaims{}
		parsed, err := jwt.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) { return s.secret, nil })
		if err != nil || !parsed.Valid {
			return s.fail(c, 401, "UNAUTHORIZED", "Invalid or missing access token")
		}
		c.Locals("user_id", claims["sub"])
		c.Locals("plan", claims["plan"])
		return c.Next()
	}
}

func (s *state) rateLimit() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if c.Path() == "/metrics" {
			return c.Next()
		}
		limit := int64(100)
		if c.Locals("plan") == "pro" || c.Locals("plan") == "team" {
			limit = 1000
		}
		ctx, cancel := requestContext(c)
		defer cancel()
		key := "rate:" + s.service + ":" + userID(c)
		count, _ := s.redis.Incr(ctx, key).Result()
		if count == 1 {
			_ = s.redis.Expire(ctx, key, time.Hour).Err()
		}
		if count > limit {
			return s.fail(c, 429, "RATE_LIMIT_EXCEEDED", "Rate limit exceeded")
		}
		return c.Next()
	}
}

func (s *state) metrics() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		path := c.Route().Path
		if path == "" {
			path = c.Path()
		}
		s.requests.WithLabelValues(c.Method(), path, strconv.Itoa(c.Response().StatusCode())).Inc()
		s.duration.WithLabelValues(c.Method(), path).Observe(time.Since(start).Seconds())
		return err
	}
}

func (s *state) errorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError
	var fiberErr *fiber.Error
	if errors.As(err, &fiberErr) {
		code = fiberErr.Code
	}
	s.logger.Error("request failed", zap.String("request_id", c.GetRespHeader(fiber.HeaderXRequestID)), zap.String("user_id", userID(c)), zap.String("service_name", s.service), zap.Error(err))
	return s.fail(c, code, "INTERNAL_ERROR", "Internal server error")
}

func (s *state) ok(c *fiber.Ctx, data any) error {
	return c.JSON(fiber.Map{"success": true, "data": data})
}
func (s *state) fail(c *fiber.Ctx, status int, code, message string) error {
	return c.Status(status).JSON(fiber.Map{"success": false, "error": apiError{Code: code, Message: message}})
}
func userID(c *fiber.Ctx) string { v, _ := c.Locals("user_id").(string); return v }
func requestContext(c *fiber.Ctx) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.UserContext(), 10*time.Second)
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
