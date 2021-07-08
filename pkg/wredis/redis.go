package wredis

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"

	"redis-bigkeys/pkg/config"
)

var (
	initOnce sync.Once
	client   *redis.Client
)

func NewClient() *redis.Client {
	if client != nil {
		return client
	}

	initOnce.Do(func() {
		cfg := config.GetCfg()
		rdsSection := `redis`

		client = redis.NewClient(&redis.Options{
			Addr:     cfg.Section(rdsSection).Key(`host`).String(),
			Password: cfg.Section(rdsSection).Key(`password`).String(),
		})

		_, err := client.Ping(context.Background()).Result()
		if err != nil {
			panic(fmt.Sprintf("get redis connect error: %v", err))
		}
	})

	return client
}
