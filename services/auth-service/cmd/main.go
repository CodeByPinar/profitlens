package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

type appState struct {
	db         *pgxpool.Pool
	redis      *redis.Client
	logger     *zap.Logger
	jwtSecret  []byte
	service    string
	accessTTL  time.Duration
	refreshTTL time.Duration
	requests   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	published  *prometheus.CounterVec
	consumed   *prometheus.CounterVec
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	serviceName := env("SERVICE_NAME", "auth-service")
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

	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	state := &appState{
		db:         db,
		redis:      rdb,
		logger:     logger,
		jwtSecret:  []byte(env("JWT_SECRET", "change-me")),
		service:    serviceName,
		accessTTL:  time.Duration(envInt("ACCESS_TOKEN_TTL_MINUTES", 15)) * time.Minute,
		refreshTTL: time.Duration(envInt("REFRESH_TOKEN_TTL_HOURS", 168)) * time.Hour,
		requests:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "Total HTTP requests.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"method", "path", "status"}),
		duration:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request duration.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"method", "path"}),
		published:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kafka_events_published_total", Help: "Published Kafka events.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"event_type"}),
		consumed:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kafka_events_consumed_total", Help: "Consumed Kafka events.", ConstLabels: prometheus.Labels{"service": serviceName}}, []string{"event_type"}),
	}
	prometheus.MustRegister(state.requests, state.duration, state.published, state.consumed)

	app := fiber.New(fiber.Config{ErrorHandler: state.errorHandler})
	app.Use(requestid.New(), recover.New(), cors.New(), state.metrics(), state.rateLimit())
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
	app.Post("/auth/register", state.register)
	app.Post("/auth/login", state.login)
	app.Post("/auth/refresh", state.refresh)
	app.Post("/auth/logout", state.logout)

	go func() {
		if err := app.Listen(":" + env("PORT", "8001")); err != nil {
			logger.Fatal("server stopped", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	_ = app.ShutdownWithTimeout(10 * time.Second)
	_ = rdb.Close()
}

func (s *appState) register(c *fiber.Ctx) error {
	var req struct {
		Email      string `json:"email"`
		Password   string `json:"password"`
		FullName   string `json:"full_name"`
		HourlyCost string `json:"hourly_cost"`
	}
	if err := c.BodyParser(&req); err != nil {
		return s.fail(c, fiber.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
	}
	if req.Email == "" || req.Password == "" || req.FullName == "" {
		return s.fail(c, fiber.StatusBadRequest, "VALIDATION_FAILED", "Email, password, and full name are required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		return err
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	var id string
	err = s.db.QueryRow(ctx, `INSERT INTO auth.users (email, password_hash, full_name, hourly_cost) VALUES ($1,$2,$3,COALESCE(NULLIF($4,'')::numeric,0)) RETURNING id`, strings.ToLower(req.Email), string(hash), req.FullName, req.HourlyCost).Scan(&id)
	if err != nil {
		return s.fail(c, fiber.StatusConflict, "USER_ALREADY_EXISTS", "User already exists")
	}
	return s.ok(c, fiber.Map{"id": id, "email": strings.ToLower(req.Email), "full_name": req.FullName})
}

func (s *appState) login(c *fiber.Ctx) error {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.BodyParser(&req); err != nil {
		return s.fail(c, fiber.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	var user struct {
		ID           string
		Email        string
		PasswordHash string
		FullName     string
		Plan         string
	}
	err := s.db.QueryRow(ctx, `SELECT id,email,password_hash,full_name,plan FROM auth.users WHERE email=$1 AND deleted_at IS NULL`, strings.ToLower(req.Email)).Scan(&user.ID, &user.Email, &user.PasswordHash, &user.FullName, &user.Plan)
	if errors.Is(err, pgx.ErrNoRows) || bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)) != nil {
		return s.fail(c, fiber.StatusUnauthorized, "INVALID_CREDENTIALS", "Invalid email or password")
	}
	access, err := s.signAccess(user.ID, user.Plan)
	if err != nil {
		return err
	}
	refresh, err := s.createRefreshToken(ctx, user.ID)
	if err != nil {
		return err
	}
	return s.ok(c, fiber.Map{"access_token": access, "refresh_token": refresh, "token_type": "Bearer", "expires_in": int(s.accessTTL.Seconds()), "user": fiber.Map{"id": user.ID, "email": user.Email, "full_name": user.FullName, "plan": user.Plan}})
}

func (s *appState) refresh(c *fiber.Ctx) error {
	var req struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := c.BodyParser(&req); err != nil || req.RefreshToken == "" {
		return s.fail(c, fiber.StatusBadRequest, "INVALID_REQUEST", "Refresh token is required")
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	var userID, plan string
	err := s.db.QueryRow(ctx, `SELECT u.id,u.plan FROM auth.refresh_tokens rt JOIN auth.users u ON u.id=rt.user_id WHERE rt.token_hash=$1 AND rt.expires_at>now() AND rt.revoked_at IS NULL AND rt.deleted_at IS NULL`, tokenHash(req.RefreshToken)).Scan(&userID, &plan)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fail(c, fiber.StatusUnauthorized, "INVALID_REFRESH_TOKEN", "Invalid refresh token")
	}
	if err != nil {
		return err
	}
	access, err := s.signAccess(userID, plan)
	if err != nil {
		return err
	}
	return s.ok(c, fiber.Map{"access_token": access, "token_type": "Bearer", "expires_in": int(s.accessTTL.Seconds())})
}

func (s *appState) logout(c *fiber.Ctx) error {
	var req struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := c.BodyParser(&req); err != nil || req.RefreshToken == "" {
		return s.fail(c, fiber.StatusBadRequest, "INVALID_REQUEST", "Refresh token is required")
	}
	ctx, cancel := requestContext(c)
	defer cancel()
	_, err := s.db.Exec(ctx, `UPDATE auth.refresh_tokens SET revoked_at=now(), updated_at=now() WHERE token_hash=$1 AND revoked_at IS NULL`, tokenHash(req.RefreshToken))
	if err != nil {
		return err
	}
	return s.ok(c, fiber.Map{"logged_out": true})
}

func (s *appState) signAccess(userID, plan string) (string, error) {
	claims := jwt.MapClaims{"sub": userID, "plan": plan, "exp": time.Now().Add(s.accessTTL).Unix(), "iat": time.Now().Unix()}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtSecret)
}

func (s *appState) createRefreshToken(ctx context.Context, userID string) (string, error) {
	token := fmt.Sprintf("%s.%d", tokenHash(userID+time.Now().String()), time.Now().UnixNano())
	_, err := s.db.Exec(ctx, `INSERT INTO auth.refresh_tokens (user_id, token_hash, expires_at) VALUES ($1,$2,$3)`, userID, tokenHash(token), time.Now().Add(s.refreshTTL))
	return token, err
}

func (s *appState) rateLimit() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if c.Path() == "/metrics" {
			return c.Next()
		}
		key := "rate:auth:" + c.IP()
		ctx, cancel := requestContext(c)
		defer cancel()
		count, _ := s.redis.Incr(ctx, key).Result()
		if count == 1 {
			_ = s.redis.Expire(ctx, key, time.Hour).Err()
		}
		if count > 1000 {
			return s.fail(c, fiber.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED", "Rate limit exceeded")
		}
		return c.Next()
	}
}

func (s *appState) metrics() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		path := c.Route().Path
		if path == "" {
			path = c.Path()
		}
		status := strconv.Itoa(c.Response().StatusCode())
		s.requests.WithLabelValues(c.Method(), path, status).Inc()
		s.duration.WithLabelValues(c.Method(), path).Observe(time.Since(start).Seconds())
		return err
	}
}

func (s *appState) errorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError
	var fiberErr *fiber.Error
	if errors.As(err, &fiberErr) {
		code = fiberErr.Code
	}
	s.logger.Error("request failed", zap.String("request_id", c.GetRespHeader(fiber.HeaderXRequestID)), zap.String("service_name", s.service), zap.Error(err))
	return s.fail(c, code, "INTERNAL_ERROR", "Internal server error")
}

func (s *appState) ok(c *fiber.Ctx, data any) error {
	return c.JSON(fiber.Map{"success": true, "data": data})
}

func (s *appState) fail(c *fiber.Ctx, status int, code, message string) error {
	return c.Status(status).JSON(fiber.Map{"success": false, "error": apiError{Code: code, Message: message}})
}

func requestContext(c *fiber.Ctx) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.UserContext(), 10*time.Second)
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(env(key, ""))
	if err != nil {
		return fallback
	}
	return value
}
