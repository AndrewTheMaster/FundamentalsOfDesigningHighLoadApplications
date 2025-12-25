package main

import (
	"log/slog"
	"lsmdb/pkg/config"
	"os"

	"github.com/goccy/go-yaml"
)

// initConfig загружает конфиг из файла YAML. Если ф��йл не найден, возвращается config.Default().
func initConfig(path string) (config.Config, error) {
	var cfg config.Config

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("config file not found, using default config", "path", path)
			return config.Default(), nil
		}
		return cfg, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// initLogger настраивает глобальный slog.Logger (JSON или текстовый).
func initLogger(cfg *config.Config) {
	var handler slog.Handler
	if cfg.Logger.JSON {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{AddSource: true})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: true})
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	slog.Info("logger initialized", "level", cfg.Logger.Level, "json", cfg.Logger.JSON)
}
