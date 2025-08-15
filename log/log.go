package log

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"

	"github.com/cpucorecore/ipfs_pin_service/internal/config"
)

var Log *zap.Logger

func InitLoggerForTest() {
	Log, _ = zap.NewDevelopment()
}

// InitLoggerWithConfig initializes logger using project config without a global G.
func InitLoggerWithConfig(cfg *config.Config) {
	if cfg == nil || !cfg.Log.Async {
		Log, _ = zap.NewDevelopment()
		return
	}

	buffer := &zapcore.BufferedWriteSyncer{
		Size:          int(cfg.Log.BufferSize.Int64()),
		FlushInterval: cfg.Log.FlushInterval,
		WS:            os.Stdout,
	}
	writeSyncer := zapcore.AddSync(buffer)

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logLevel, err := zapcore.ParseLevel(cfg.Log.Level)
	if err != nil {
		panic(fmt.Sprintf("log level error: %v, level:[%s]", err, cfg.Log.Level))
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		writeSyncer,
		logLevel,
	)

	Log = zap.New(core)
}
