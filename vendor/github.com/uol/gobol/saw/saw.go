package saw

import (
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(logLevel, env string) (*zap.Logger, error) {

	var cfg zap.Config

	switch env {
	case "QA":
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	case "PROD":
		cfg = zap.NewProductionConfig()
		//cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	err := cfg.Level.UnmarshalText([]byte(strings.ToLower(logLevel)))
	if err != nil {
		return nil, err
	}

	return cfg.Build()
}