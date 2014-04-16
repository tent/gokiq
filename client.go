package gokiq

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

var Client = NewClientConfig()

type jobMap map[reflect.Type]JobConfig

type ClientConfig struct {
	RedisPool      *redis.Pool
	RedisNamespace string

	TestMode  bool
	TestQueue []interface{}

	jobMapping  jobMap
	knownQueues map[string]struct{}
	initOnce    sync.Once
	mtx         sync.Mutex
}

func NewClientConfig() *ClientConfig {
	return &ClientConfig{
		jobMapping:  make(jobMap),
		knownQueues: make(map[string]struct{}),
	}
}

func (c *ClientConfig) Register(worker interface{}, queue string, retries int) {
	t := workerType(worker)
	c.jobMapping[t] = JobConfig{Queue: queue, MaxRetries: retries, Name: t.Name()}
	c.trackQueue(queue)
}

func (c *ClientConfig) RegisterName(name string, worker interface{}, queue string, retries int) {
	c.jobMapping[workerType(worker)] = JobConfig{Queue: queue, MaxRetries: retries, Name: name}
	c.trackQueue(queue)
}

func (c *ClientConfig) init() {
	if c.RedisPool == nil {
		c.RedisPool = redis.NewPool(func() (redis.Conn, error) {
			return redis.Dial("tcp", defaultRedisServer)
		}, 1)
	}
	queues := make([]interface{}, 1, len(c.knownQueues)+1)
	queues[0] = c.nsKey("queues")
	for queue := range c.knownQueues {
		queues = append(queues, queue)
	}
	c.redisQuery("SADD", queues...)
}

func (c *ClientConfig) QueueJob(worker interface{}) error {
	c.initOnce.Do(func() { c.init() })
	config, ok := c.jobMapping[workerType(worker)]
	if !ok {
		panic(fmt.Errorf("gokiq: Unregistered worker type %T", worker))
	}
	return c.queueJob(worker, config)
}

func (c *ClientConfig) QueueJobConfig(worker interface{}, config JobConfig) error {
	c.initOnce.Do(func() { c.init() })
	if baseConfig, ok := c.jobMapping[workerType(worker)]; ok {
		if config.Name == "" {
			config.Name = baseConfig.Name
		}
		if config.Queue == "" {
			config.Queue = baseConfig.Queue
		}
	}
	c.trackQueue(config.Queue)
	return c.queueJob(worker, config)
}

func (c *ClientConfig) queueJob(worker interface{}, config JobConfig) error {
	if c.TestMode {
		c.TestQueue = append(c.TestQueue, worker)
		return nil
	}

	data, err := json.Marshal(worker)
	if err != nil {
		return err
	}
	args := json.RawMessage(data)
	job := &Job{
		Type:  config.Name,
		Args:  &args,
		Retry: config.MaxRetries,
		ID:    uuid(),
	}

	if config.At.IsZero() {
		_, err = c.redisQuery("RPUSH", c.nsKey("queue:"+config.Queue), job.JSON())
	} else {
		job.Queue = config.Queue
		_, err = c.redisQuery("ZADD", c.nsKey("schedule"), timeFloat(config.At), job.JSON())
	}
	return err
}

func (c *ClientConfig) trackQueue(queue string) {
	c.mtx.Lock()
	if _, ok := c.knownQueues[queue]; !ok {
		c.knownQueues[queue] = struct{}{}
		if c.RedisPool != nil {
			c.redisQuery("SADD", c.nsKey("queues"), queue)
		}
	}
	c.mtx.Unlock()
}

func (c *ClientConfig) redisQuery(command string, args ...interface{}) (interface{}, error) {
	conn := c.RedisPool.Get()
	defer conn.Close()
	return conn.Do(command, args...)
}

func (c *ClientConfig) nsKey(key string) string {
	if c.RedisNamespace != "" {
		return c.RedisNamespace + ":" + key
	}
	return key
}

func uuid() string {
	var id [16]byte
	if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
		panic(err)
	}
	id[6] &= 0x0F // clear version
	id[6] |= 0x40 // set version to 4 (random uuid)
	id[8] &= 0x3F // clear variant
	id[8] |= 0x80 // set to IETF variant
	return hex.EncodeToString(id[:])
}

type JobConfig struct {
	Name       string
	Queue      string
	MaxRetries int
	At         time.Time
}
