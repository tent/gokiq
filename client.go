package gokiq

import (
	"crypto/rand"
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
	RedisServer    string
	RedisNamespace string
	RedisMaxIdle   int
	Fake           bool

	redisPool   *redis.Pool
	jobMapping  jobMap
	knownQueues map[string]struct{}
	mtx         sync.Mutex
}

func NewClientConfig() *ClientConfig {
	return &ClientConfig{
		RedisServer:  defaultRedisServer,
		RedisMaxIdle: 1,
		jobMapping:   make(jobMap),
		knownQueues:  make(map[string]struct{}),
	}
}

func (c *ClientConfig) Register(worker Worker, queue string, retries int) {
	t := workerType(worker)
	c.jobMapping[t] = JobConfig{Queue: queue, MaxRetries: retries, Name: t.Name()}
	c.trackQueue(queue)
}

func (c *ClientConfig) RegisterName(name string, worker Worker, queue string, retries int) {
	c.jobMapping[workerType(worker)] = JobConfig{Queue: queue, MaxRetries: retries, Name: name}
	c.trackQueue(queue)
}

func (c *ClientConfig) Connect() {
	// TODO: add a mutex for the redis pool
	if c.redisPool != nil {
		c.redisPool.Close()
	}
	c.redisPool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", c.RedisServer)
	}, c.RedisMaxIdle)

	queues := make([]interface{}, 1, len(c.knownQueues)+1)
	queues[0] = c.nsKey("queues")
	for queue := range c.knownQueues {
		queues = append(queues, queue)
	}
	c.redisQuery("SADD", queues...)
}

func (c *ClientConfig) QueueJob(worker Worker) error {
	config, ok := c.jobMapping[workerType(worker)]
	if !ok {
		panic(fmt.Errorf("gokiq: Unregistered worker type %T", worker))
	}
	return c.queueJob(worker, config)
}

func (c *ClientConfig) QueueJobConfig(worker Worker, config JobConfig) error {
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

func (c *ClientConfig) queueJob(worker Worker, config JobConfig) error {
	data, err := json.Marshal(worker)
	if err != nil {
		return err
	}
	args := json.RawMessage(data)
	job := &Job{
		Type:  config.Name,
		Args:  &args,
		Retry: config.MaxRetries,
		ID:    generateJobID(),
	}
	if c.Fake {
		return worker.Perform()
	}

	if config.At != nil {
		job.Queue = config.Queue
		_, err = c.redisQuery("ZADD", c.nsKey("schedule"), timeFloat(*config.At), job.JSON())
	} else {
		_, err = c.redisQuery("RPUSH", c.nsKey("queue:"+config.Queue), job.JSON())
	}
	return err
}

func (c *ClientConfig) trackQueue(queue string) {
	c.mtx.Lock()
	if _, ok := c.knownQueues[queue]; !ok {
		c.knownQueues[queue] = struct{}{}
		if c.redisPool != nil {
			c.redisQuery("SADD", c.nsKey("queues"), queue)
		}
	}
	c.mtx.Unlock()
}

func (c *ClientConfig) redisQuery(command string, args ...interface{}) (interface{}, error) {
	conn := c.redisPool.Get()
	defer conn.Close()
	return conn.Do(command, args...)
}

func (c *ClientConfig) nsKey(key string) string {
	if c.RedisNamespace != "" {
		return c.RedisNamespace + ":" + key
	}
	return key
}

func generateJobID() string {
	b := make([]byte, 8)
	io.ReadFull(rand.Reader, b)
	return fmt.Sprintf("%x", b)
}

type JobConfig struct {
	Name       string
	Queue      string
	MaxRetries int
	At         *time.Time
}
