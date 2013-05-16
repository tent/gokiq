package gokiq

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

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
	c.jobMapping[t] = JobConfig{queue, retries, t.Name()}
	c.trackQueue(queue)
}

func (c *ClientConfig) RegisterName(name string, worker Worker, queue string, retries int) {
	c.jobMapping[workerType(worker)] = JobConfig{queue, retries, name}
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
		Type:  config.name,
		Args:  &args,
		Retry: config.MaxRetries,
		ID:    generateJobID(),
	}
	if c.Fake {
		return worker.Perform()
	}

	_, err = c.redisQuery("RPUSH", c.nsKey("queue:"+config.Queue), job.JSON())
	return err
}

func (c *ClientConfig) trackQueue(queue string) {
	_, known := c.knownQueues[queue]
	if !known {
		c.knownQueues[queue] = struct{}{}
		if c.redisPool != nil {
			c.redisQuery("SADD", c.nsKey("queues"), queue)
		}
	}
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
	Queue      string
	MaxRetries int

	name string
}
