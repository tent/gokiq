package gokiq

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/garyburd/redigo/redis"
)

var Client = NewClientConfig()

type jobMap map[reflect.Type]JobConfig

type ClientConfig struct {
	RedisServer    string
	RedisNamespace string
	RedisMaxIdle   int

	redisPool  *redis.Pool
	jobMapping jobMap
}

func NewClientConfig() *ClientConfig {
	return &ClientConfig{
		RedisServer:  defaultRedisServer,
		RedisMaxIdle: 1,
		jobMapping:   make(jobMap),
	}
}

func (c *ClientConfig) Register(name string, worker Worker, queue string, retries int) {
	c.jobMapping[workerType(worker)] = JobConfig{queue, retries, name}
}

func (c *ClientConfig) Connect() {
	// TODO: add a mutex for the redis pool
	if c.redisPool != nil {
		c.redisPool.Close()
	}
	c.redisPool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", c.RedisServer)
	}, c.RedisMaxIdle)
}

func (c *ClientConfig) QueueJob(worker Worker, args ...interface{}) error {
	config, ok := c.jobMapping[workerType(worker)]
	if !ok {
		panic(fmt.Errorf("gokiq: Unregistered worker type %T", worker))
	}
	return c.queueJob(config.name, config, args)
}

func (c *ClientConfig) QueueJobWithConfig(name string, config JobConfig, args ...interface{}) error {
	return c.queueJob(name, config, args)
}

func (c *ClientConfig) queueJob(name string, config JobConfig, args []interface{}) error {
	if args == nil {
		args = make([]interface{}, 0) // json encodes nil slices as null
	}
	job := &Job{
		Type:  name,
		Args:  args,
		Retry: config.MaxRetries,
		ID:    generateJobID(),
	}
	json, err := json.Marshal(job)
	if err != nil {
		return err
	}

	conn := c.redisPool.Get()
	defer conn.Close()
	conn.Send("SADD", c.nsKey("queues"), config.Queue)
	_, err = conn.Do("RPUSH", c.nsKey("queue:"+config.Queue), json)

	return err
}

func (c *ClientConfig) nsKey(key string) string {
	if c.RedisNamespace != "" {
		return c.RedisNamespace + ":" + key
	}
	return key
}

func generateJobID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

type JobConfig struct {
	Queue      string
	MaxRetries int

	name string
}
