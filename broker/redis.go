package broker

import (
	"github.com/go-redis/redis"
)

type RedisConfig struct {
	host     string
	port     string
	password string
	db       int
}

func NewRedisConfig() *RedisConfig {
	return &RedisConfig{
		host:     "127,0.0.1",
		port:     "6379",
		password: "",
		db:       0,
	}
}

type RedisBroker struct {
	client *redis.Client
}

func NewRedisBroker() *RedisBroker {
	return &RedisBroker{
		client: redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB)
		}),
	}
}

func (this *RedisBroker) Init() {
	this.client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB)
	})
}

func (this *RedisBroker) Set(key string, field string, job []byte) {
	this.client.HSet(key, field, job)
}

func (this *RedisBroker) Get(key string, field string) (job []byte) {
	job, _ = this.client.HGet(key, field).Bytes()
	return
}

func (this *RedisBroker) Delete(key string, field string) {
	this.client.HDel(key, field)
}
