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
	"github.com/jackc/pgx/v5"
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
	kafka     *kafka.Writer
	logger    *zap.Logger
	secret    []byte
	service   string
	requests  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	published *prometheus.CounterVec
	consumed  *prometheus.CounterVec
}

type event struct {
	EventType  string `json:"event_type"`
	Payload    any    `json:"payload"`
	OccurredAt string `json:"occurred_at"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	serviceName := env("SERVICE_NAME", "project-service")
	jwtSecret, err := requiredEnv("JWT_SECRET")
	if err != nil {
		logger.Fatal("missing required configuration", zap.Error(err))
	}
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
		kafka:     &kafka.Writer{Addr: kafka.TCP(strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")...), Topic: env("KAFKA_TOPIC", "profitlens.events"), Balancer: &kafka.LeastBytes{}},
		logger:    logger,
		secret:    []byte(jwtSecret),
		service:   serviceName,
		requests:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "Total HTTP requests.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"method", "path", "status"}),
		duration:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request duration.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"method", "path"}),
		published: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kafka_events_published_total", Help: "Published Kafka events.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"event_type"}),
		consumed:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kafka_events_consumed_total", Help: "Consumed Kafka events.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"event_type"}),
	}
	prometheus.MustRegister(s.requests, s.duration, s.published, s.consumed)
	app := fiber.New(fiber.Config{ErrorHandler: s.errorHandler})
	app.Use(requestid.New(), recover.New(), cors.New(), s.metrics(), s.auth(), s.rateLimit())
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
	app.Post("/clients", s.createClient)
	app.Get("/clients", s.listClients)
	app.Get("/clients/:id", s.getClient)
	app.Get("/clients/:id/risk", s.getRisk)
	app.Post("/projects", s.createProject)
	app.Get("/projects", s.listProjects)
	app.Get("/projects/:id", s.getProject)
	app.Patch("/projects/:id/status", s.updateProjectStatus)
	app.Delete("/projects/:id", s.deleteProject)
	app.Post("/projects/:id/time-entries", s.createTimeEntry)
	app.Get("/projects/:id/time-entries", s.listTimeEntries)
	app.Post("/projects/:id/expenses", s.createExpense)
	app.Get("/projects/:id/expenses", s.listExpenses)
	app.Get("/projects/:id/profitability", s.getProfitability)
	go func() {
		if err := app.Listen(":" + env("PORT", "8002")); err != nil {
			logger.Fatal("server stopped", zap.Error(err))
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	_ = app.ShutdownWithTimeout(10 * time.Second)
	_ = s.kafka.Close()
	_ = s.redis.Close()
}

func (s *state) createClient(c *fiber.Ctx) error {
	var req struct {
		Name     string `json:"name"`
		Country  string `json:"country"`
		Currency string `json:"currency"`
	}
	if err := c.BodyParser(&req); err != nil || req.Name == "" {
		return s.fail(c, 400, "VALIDATION_FAILED", "Client name is required")
	}
	if req.Currency == "" {
		req.Currency = "TRY"
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	var id string
	err := s.db.QueryRow(ctx, `INSERT INTO project.clients (user_id,name,country,currency) VALUES ($1,$2,$3,$4) RETURNING id`, userID(c), req.Name, req.Country, req.Currency).Scan(&id)
	if err != nil {
		return err
	}
	return s.ok(c, fiber.Map{"id": id, "name": req.Name})
}

func (s *state) listClients(c *fiber.Ctx) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	rows, err := s.db.Query(ctx, `SELECT id,name,country,currency,risk_score,created_at FROM project.clients WHERE user_id=$1 AND deleted_at IS NULL ORDER BY created_at DESC`, userID(c))
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []fiber.Map{}
	for rows.Next() {
		var id, name, currency string
		var country *string
		var risk int
		var created time.Time
		if err := rows.Scan(&id, &name, &country, &currency, &risk, &created); err != nil {
			return err
		}
		items = append(items, fiber.Map{"id": id, "name": name, "country": country, "currency": currency, "risk_score": risk, "created_at": created})
	}
	return s.ok(c, items)
}

func (s *state) getClient(c *fiber.Ctx) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	var id, name, currency string
	var country *string
	var risk int
	err := s.db.QueryRow(ctx, `SELECT id,name,country,currency,risk_score FROM project.clients WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL`, c.Params("id"), userID(c)).Scan(&id, &name, &country, &currency, &risk)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fail(c, 404, "CLIENT_NOT_FOUND", "Client not found")
	}
	if err != nil {
		return err
	}
	return s.ok(c, fiber.Map{"id": id, "name": name, "country": country, "currency": currency, "risk_score": risk})
}

func (s *state) getRisk(c *fiber.Ctx) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	key := "risk:client:" + c.Params("id")
	if cached, err := s.redis.Get(ctx, key).Result(); err == nil {
		return s.ok(c, fiber.Map{"client_id": c.Params("id"), "risk_score": cached})
	}
	var risk int
	err := s.db.QueryRow(ctx, `SELECT risk_score FROM project.clients WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL`, c.Params("id"), userID(c)).Scan(&risk)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fail(c, 404, "CLIENT_NOT_FOUND", "Client not found")
	}
	if err != nil {
		return err
	}
	_ = s.redis.Set(ctx, key, strconv.Itoa(risk), time.Hour).Err()
	return s.ok(c, fiber.Map{"client_id": c.Params("id"), "risk_score": risk})
}

func (s *state) createProject(c *fiber.Ctx) error {
	var req struct {
		ClientID    string `json:"client_id"`
		Name        string `json:"name"`
		Type        string `json:"type"`
		Currency    string `json:"currency"`
		BudgetCents int64  `json:"budget_cents"`
	}
	if err := c.BodyParser(&req); err != nil || req.ClientID == "" || req.Name == "" {
		return s.fail(c, 400, "VALIDATION_FAILED", "Client id and project name are required")
	}
	if req.Currency == "" {
		req.Currency = "TRY"
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	var id string
	err := s.db.QueryRow(ctx, `INSERT INTO project.projects (client_id,name,type,budget_cents,currency) SELECT $1,$2,$3,$4,$5 WHERE EXISTS (SELECT 1 FROM project.clients WHERE id=$1 AND user_id=$6 AND deleted_at IS NULL) RETURNING id`, req.ClientID, req.Name, req.Type, req.BudgetCents, req.Currency, userID(c)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fail(c, 404, "CLIENT_NOT_FOUND", "Client not found")
	}
	if err != nil {
		return err
	}
	return s.ok(c, fiber.Map{"id": id, "name": req.Name})
}

func (s *state) listProjects(c *fiber.Ctx) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	rows, err := s.db.Query(ctx, `SELECT p.id,p.client_id,p.name,p.type,p.budget_cents,p.status,p.currency FROM project.projects p JOIN project.clients c ON c.id=p.client_id WHERE c.user_id=$1 AND p.deleted_at IS NULL ORDER BY p.created_at DESC`, userID(c))
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []fiber.Map{}
	for rows.Next() {
		var id, clientID, name, typ, status, currency string
		var budget int64
		if err := rows.Scan(&id, &clientID, &name, &typ, &budget, &status, &currency); err != nil {
			return err
		}
		items = append(items, fiber.Map{"id": id, "client_id": clientID, "name": name, "type": typ, "budget_cents": budget, "status": status, "currency": currency})
	}
	return s.ok(c, items)
}

func (s *state) getProject(c *fiber.Ctx) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	var id, clientID, name, typ, status, currency string
	var budget int64
	err := s.db.QueryRow(ctx, `SELECT p.id,p.client_id,p.name,p.type,p.budget_cents,p.status,p.currency FROM project.projects p JOIN project.clients c ON c.id=p.client_id WHERE p.id=$1 AND c.user_id=$2 AND p.deleted_at IS NULL`, c.Params("id"), userID(c)).Scan(&id, &clientID, &name, &typ, &budget, &status, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fail(c, 404, "PROJECT_NOT_FOUND", "Project not found")
	}
	if err != nil {
		return err
	}
	return s.ok(c, fiber.Map{"id": id, "client_id": clientID, "name": name, "type": typ, "budget_cents": budget, "status": status, "currency": currency})
}

func (s *state) updateProjectStatus(c *fiber.Ctx) error {
	var req struct {
		Status string `json:"status"`
	}
	if err := c.BodyParser(&req); err != nil || req.Status == "" {
		return s.fail(c, 400, "VALIDATION_FAILED", "Status is required")
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	tag, err := s.db.Exec(ctx, `UPDATE project.projects p SET status=$1, updated_at=now() FROM project.clients c WHERE c.id=p.client_id AND p.id=$2 AND c.user_id=$3 AND p.deleted_at IS NULL`, req.Status, c.Params("id"), userID(c))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return s.fail(c, 404, "PROJECT_NOT_FOUND", "Project not found")
	}
	if req.Status == "completed" {
		_ = s.publish(ctx, "project.closed", fiber.Map{"project_id": c.Params("id"), "user_id": userID(c)})
	}
	return s.ok(c, fiber.Map{"id": c.Params("id"), "status": req.Status})
}

func (s *state) deleteProject(c *fiber.Ctx) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	tag, err := s.db.Exec(ctx, `UPDATE project.projects p SET deleted_at=now(), updated_at=now() FROM project.clients c WHERE c.id=p.client_id AND p.id=$1 AND c.user_id=$2 AND p.deleted_at IS NULL`, c.Params("id"), userID(c))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return s.fail(c, 404, "PROJECT_NOT_FOUND", "Project not found")
	}
	return s.ok(c, fiber.Map{"deleted": true})
}

func (s *state) createTimeEntry(c *fiber.Ctx) error {
	var req struct {
		Hours           string `json:"hours"`
		HourlyRateCents int64  `json:"hourly_rate_cents"`
		Description     string `json:"description"`
		EntryDate       string `json:"entry_date"`
	}
	if err := c.BodyParser(&req); err != nil || req.Hours == "" || req.HourlyRateCents <= 0 || req.EntryDate == "" {
		return s.fail(c, 400, "VALIDATION_FAILED", "Hours, hourly rate, and entry date are required")
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	var id string
	err := s.db.QueryRow(ctx, `INSERT INTO project.time_entries (project_id,user_id,hours,hourly_rate_cents,description,entry_date) SELECT $1,$2,$3::numeric,$4,$5,$6::date WHERE EXISTS (SELECT 1 FROM project.projects p JOIN project.clients c ON c.id=p.client_id WHERE p.id=$1 AND c.user_id=$2 AND p.deleted_at IS NULL) RETURNING id`, c.Params("id"), userID(c), req.Hours, req.HourlyRateCents, req.Description, req.EntryDate).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fail(c, 404, "PROJECT_NOT_FOUND", "Project not found")
	}
	if err != nil {
		return err
	}
	_ = s.publish(ctx, "time.entry.created", fiber.Map{"project_id": c.Params("id"), "time_entry_id": id, "user_id": userID(c)})
	return s.ok(c, fiber.Map{"id": id})
}

func (s *state) listTimeEntries(c *fiber.Ctx) error {
	return s.listRows(c, `SELECT te.id,te.hours::text,te.hourly_rate_cents,te.description,te.entry_date FROM project.time_entries te JOIN project.projects p ON p.id=te.project_id JOIN project.clients cl ON cl.id=p.client_id WHERE te.project_id=$1 AND cl.user_id=$2 AND te.deleted_at IS NULL ORDER BY te.entry_date DESC`, "time_entries")
}

func (s *state) createExpense(c *fiber.Ctx) error {
	var req struct {
		Category    string `json:"category"`
		AmountCents int64  `json:"amount_cents"`
		Currency    string `json:"currency"`
		Description string `json:"description"`
		ExpenseDate string `json:"expense_date"`
	}
	if err := c.BodyParser(&req); err != nil || req.AmountCents <= 0 || req.ExpenseDate == "" {
		return s.fail(c, 400, "VALIDATION_FAILED", "Amount and expense date are required")
	}
	if req.Currency == "" {
		req.Currency = "TRY"
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	var id string
	err := s.db.QueryRow(ctx, `INSERT INTO project.expenses (project_id,category,amount_cents,currency,description,expense_date) SELECT $1,$2,$3,$4,$5,$6::date WHERE EXISTS (SELECT 1 FROM project.projects p JOIN project.clients c ON c.id=p.client_id WHERE p.id=$1 AND c.user_id=$7 AND p.deleted_at IS NULL) RETURNING id`, c.Params("id"), req.Category, req.AmountCents, req.Currency, req.Description, req.ExpenseDate, userID(c)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fail(c, 404, "PROJECT_NOT_FOUND", "Project not found")
	}
	if err != nil {
		return err
	}
	_ = s.publish(ctx, "expense.added", fiber.Map{"project_id": c.Params("id"), "expense_id": id, "user_id": userID(c)})
	return s.ok(c, fiber.Map{"id": id})
}

func (s *state) listExpenses(c *fiber.Ctx) error {
	return s.listRows(c, `SELECT e.id,e.category,e.amount_cents,e.description,e.expense_date FROM project.expenses e JOIN project.projects p ON p.id=e.project_id JOIN project.clients cl ON cl.id=p.client_id WHERE e.project_id=$1 AND cl.user_id=$2 AND e.deleted_at IS NULL ORDER BY e.expense_date DESC`, "expenses")
}

func (s *state) listRows(c *fiber.Ctx, query, name string) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	rows, err := s.db.Query(ctx, query, c.Params("id"), userID(c))
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []fiber.Map{}
	for rows.Next() {
		if name == "time_entries" {
			var id, hours, desc string
			var rate int64
			var date time.Time
			if err := rows.Scan(&id, &hours, &rate, &desc, &date); err != nil {
				return err
			}
			items = append(items, fiber.Map{"id": id, "hours": hours, "hourly_rate_cents": rate, "description": desc, "entry_date": date.Format("2006-01-02")})
		} else {
			var id, category, desc string
			var amount int64
			var date time.Time
			if err := rows.Scan(&id, &category, &amount, &desc, &date); err != nil {
				return err
			}
			items = append(items, fiber.Map{"id": id, "category": category, "amount_cents": amount, "description": desc, "expense_date": date.Format("2006-01-02")})
		}
	}
	return s.ok(c, items)
}

func (s *state) getProfitability(c *fiber.Ctx) error {
	ctx, cancel := requestContext(c)
	defer cancel()
	key := "profit:project:" + c.Params("id")
	if cached, err := s.redis.Get(ctx, key).Result(); err == nil {
		var data fiber.Map
		_ = json.Unmarshal([]byte(cached), &data)
		return s.ok(c, data)
	}
	var revenue, cost, net int64
	var margin string
	err := s.db.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT SUM(i.amount_cents) FROM billing.invoices i WHERE i.project_id=p.id AND i.status='paid' AND i.deleted_at IS NULL),0)::bigint AS revenue_cents,
			(
				COALESCE((SELECT SUM((te.hours * u.hourly_cost * 100)::bigint) FROM project.time_entries te JOIN auth.users u ON u.id=te.user_id WHERE te.project_id=p.id AND te.deleted_at IS NULL),0)::bigint
				+ COALESCE((SELECT SUM(e.amount_cents) FROM project.expenses e WHERE e.project_id=p.id AND e.deleted_at IS NULL),0)::bigint
			) AS cost_cents
		FROM project.projects p
		JOIN project.clients c ON c.id=p.client_id
		WHERE p.id=$1 AND c.user_id=$2 AND p.deleted_at IS NULL`, c.Params("id"), userID(c)).Scan(&revenue, &cost)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fail(c, 404, "PROJECT_NOT_FOUND", "Project not found")
	}
	if err != nil {
		return err
	}
	net = revenue - cost
	margin = "0.00"
	if revenue > 0 {
		margin = formatPercent((net * 10000) / revenue)
	}
	data := fiber.Map{"project_id": c.Params("id"), "revenue_cents": revenue, "cost_cents": cost, "net_profit_cents": net, "margin_percent": margin}
	raw, _ := json.Marshal(data)
	_ = s.redis.Set(ctx, key, raw, 5*time.Minute).Err()
	return s.ok(c, data)
}

func (s *state) publish(ctx context.Context, eventType string, payload any) error {
	raw, _ := json.Marshal(event{EventType: eventType, Payload: payload, OccurredAt: time.Now().UTC().Format(time.RFC3339)})
	err := s.kafka.WriteMessages(ctx, kafka.Message{Key: []byte(eventType), Value: raw})
	if err == nil {
		s.published.WithLabelValues(eventType).Inc()
	}
	return err
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

func requiredEnv(key string) (string, error) {
	if v := os.Getenv(key); v != "" {
		return v, nil
	}
	return "", errors.New(key + " is required")
}

func formatPercent(basisPoints int64) string {
	sign := ""
	if basisPoints < 0 {
		sign = "-"
		basisPoints = -basisPoints
	}
	return sign + strconv.FormatInt(basisPoints/100, 10) + "." + leftPad2(strconv.FormatInt(basisPoints%100, 10))
}

func leftPad2(value string) string {
	if len(value) == 1 {
		return "0" + value
	}
	return value
}
