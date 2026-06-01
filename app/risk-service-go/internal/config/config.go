package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	// Config хранит уже распарсенные значения, чтобы остальной код не читал os.Getenv напрямую.
	AppEnv              string
	LogLevel            string
	Host                string
	Port                int
	KafkaClientID       string
	KafkaBrokers        []string
	KafkaConsumerGroup  string
	SchemaRegistryURL   string
	RiskScoreThreshold  float64
	RiskScoreIterations int
}

func Load(serviceRoot string) (Config, error) {
	// Сначала подмешиваем .env-файлы, затем читаем все значения в строгие типы.
	loadEnvFiles(serviceRoot)

	port, err := intEnv("PORT", 3002)
	if err != nil {
		return Config{}, err
	}
	threshold, err := floatEnv("RISK_SCORE_THRESHOLD", 0.72)
	if err != nil {
		return Config{}, err
	}
	iterations, err := intEnv("RISK_SCORE_ITERATIONS", 300000)
	if err != nil {
		return Config{}, err
	}

	brokers := splitCSV(stringEnv("KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must not be empty")
	}

	return Config{
		AppEnv:              stringEnv("APP_ENV", "local"),
		LogLevel:            stringEnv("LOG_LEVEL", "info"),
		Host:                stringEnv("HOST", "0.0.0.0"),
		Port:                port,
		KafkaClientID:       stringEnv("KAFKA_CLIENT_ID", "risk-service-go"),
		KafkaBrokers:        brokers,
		KafkaConsumerGroup:  stringEnv("KAFKA_CONSUMER_GROUP_ID", "risk-service-go"),
		SchemaRegistryURL:   strings.TrimRight(stringEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"), "/"),
		RiskScoreThreshold:  threshold,
		RiskScoreIterations: iterations,
	}, nil
}

func (c Config) HTTPAddress() string {
	// net/http ожидает адрес в формате host:port.
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func loadEnvFiles(serviceRoot string) {
	// Поведение похоже на order-service: .env.local/.env.prod выбирается через APP_ENV.
	appEnv := os.Getenv("APP_ENV")
	if appEnv != "prod" {
		appEnv = "local"
	}

	// Реальные переменные окружения имеют приоритет над файлами.
	existingEnvironmentKeys := map[string]struct{}{}
	for _, env := range os.Environ() {
		key, _, ok := strings.Cut(env, "=")
		if ok {
			existingEnvironmentKeys[key] = struct{}{}
		}
	}

	for _, fileName := range []string{".env", ".env." + appEnv} {
		filePath := filepath.Join(serviceRoot, fileName)
		file, err := os.Open(filePath)
		if err != nil {
			continue
		}
		// Формат поддержан простой: KEY=value, без сложного dotenv-синтаксиса.
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			key, value, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			if _, exists := existingEnvironmentKeys[key]; !exists {
				_ = os.Setenv(key, strings.Trim(strings.TrimSpace(value), `"`))
			}
		}
		_ = file.Close()
	}
}

func stringEnv(key string, fallback string) string {
	// Пустая строка считается отсутствующим значением, чтобы сработал fallback.
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func intEnv(key string, fallback int) (int, error) {
	// Возвращаем ошибку при неверном формате, чтобы сервис падал на старте явно.
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func floatEnv(key string, fallback float64) (float64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a float: %w", key, err)
	}
	return parsed, nil
}

func splitCSV(value string) []string {
	// KAFKA_BROKERS задается как "host1:9092,host2:9092".
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
