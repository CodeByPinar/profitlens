package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type state struct {
	client    *http.Client
	redis     *redis.Client
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

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	serviceName := env("SERVICE_NAME", "gateway")
	s := &state{
		client:    &http.Client{Timeout: 30 * time.Second},
		redis:     redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")}),
		logger:    logger,
		secret:    []byte(env("JWT_SECRET", "change-me")),
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
	app.Get("/", s.docsPage)
	app.Static("/docs/assets", "docs/assets")
	app.Get("/docs", s.docsPage)
	app.Get("/docs/", s.docsPage)
	app.Get("/docs/openapi.json", s.openapiSpec)
	app.Get("/openapi.json", s.openapiSpec)
	app.All("/auth/*", s.proxy(env("AUTH_SERVICE_URL", "http://localhost:8001")))
	app.All("/clients*", s.proxy(env("PROJECT_SERVICE_URL", "http://localhost:8002")))
	app.All("/projects*", s.proxy(env("PROJECT_SERVICE_URL", "http://localhost:8002")))
	app.All("/invoices*", s.proxy(env("BILLING_SERVICE_URL", "http://localhost:8003")))
	app.All("/dashboard*", s.proxy(env("ANALYTICS_SERVICE_URL", "http://localhost:8004")))
	app.All("/reports*", s.proxy(env("ANALYTICS_SERVICE_URL", "http://localhost:8004")))
	go func() {
		if err := app.Listen(":" + env("PORT", "8000")); err != nil {
			logger.Fatal("server stopped", zap.Error(err))
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	_ = app.ShutdownWithTimeout(10 * time.Second)
	_ = s.redis.Close()
}

func (s *state) proxy(target string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx, cancel := requestContext(c)
		defer cancel()
		url := target + c.OriginalURL()
		req, err := http.NewRequestWithContext(ctx, c.Method(), url, bytes.NewReader(c.Body()))
		if err != nil {
			return err
		}
		c.Request().Header.VisitAll(func(k, v []byte) {
			key := string(k)
			if strings.EqualFold(key, "Host") {
				return
			}
			req.Header.Set(key, string(v))
		})
		req.Header.Set("X-Request-ID", c.GetRespHeader(fiber.HeaderXRequestID))
		if userID(c) != "" {
			req.Header.Set("X-User-ID", userID(c))
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return s.fail(c, 502, "BAD_GATEWAY", "Upstream service is unavailable")
		}
		defer resp.Body.Close()
		for key, values := range resp.Header {
			for _, value := range values {
				c.Set(key, value)
			}
		}
		c.Status(resp.StatusCode)
		_, err = io.Copy(c.Response().BodyWriter(), resp.Body)
		return err
	}
}

func (s *state) docsPage(c *fiber.Ctx) error {
	page, err := os.ReadFile("docs/index.html")
	if err != nil {
		return err
	}
	c.Type("html", "utf-8")
	return c.Send(page)
}

func (s *state) openapiSpec(c *fiber.Ctx) error {
	spec, err := os.ReadFile("docs/openapi.json")
	if err != nil {
		return err
	}
	c.Type("json", "utf-8")
	return c.Send(spec)
}

func (s *state) auth() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if c.Path() == "/" || c.Path() == "/metrics" || c.Path() == "/docs" || c.Path() == "/docs/" || strings.HasPrefix(c.Path(), "/docs/assets/") || c.Path() == "/docs/openapi.json" || c.Path() == "/openapi.json" || strings.HasPrefix(c.Path(), "/auth/") {
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
		subject := userID(c)
		if subject == "" {
			subject = c.IP()
		}
		ctx, cancel := requestContext(c)
		defer cancel()
		key := "rate:" + s.service + ":" + subject
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
