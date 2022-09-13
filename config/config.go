package config

import (
	"errors"
	"net"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

var (
	ErrCfgNoMembers          = errors.New("Config has no members")
	ErrCfgInvalidMember      = errors.New("Invalid member format not IP:PORT")
	ErrCfgInvalidListen      = errors.New("Invalid listen address")
	ErrCfgInvalidRedisListen = errors.New("Invalid redis listen address")
)

type Config struct {
	fname       string
	Members     []string `yaml:"members"`
	RTT         int      `yaml:"rtt,omitempty"`
	Listen      string   `yaml:"listen"`
	RedisListen string   `yaml:"redis_listen"`
	WalDir      string   `yaml:"wal_dir"`
	DBDir       string   `yaml:"db_dir"`
}

func NewConfig(fname string) (*Config, error) {
	cfg := &Config{
		fname: fname,
	}
	err := cfg.Reload()
	return cfg, err
}

func (c *Config) Reload() error {
	data, err := os.ReadFile(c.fname)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, c)
}

func (c *Config) Check() error {
	if len(c.Members) == 0 {
		return ErrCfgNoMembers
	}

	for _, mem := range c.Members {
		if !checkIPPort(mem) {
			return ErrCfgInvalidMember
		}
	}

	if c.RTT <= 0 {
		c.RTT = 100
	}

	if c.Listen == "" && !checkIPPort(c.Listen) {
		return ErrCfgInvalidListen
	}

	if c.RedisListen != "" && !checkIPPort(c.RedisListen) {
		return ErrCfgInvalidRedisListen
	}

	if c.WalDir == "" {
		c.WalDir = "/tmp/sample/wal"
	}

	return nil
}

func checkIPPort(addr string) bool {
	ip, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if ip != "" && net.ParseIP(ip) == nil {
		// Invalid IP address, empty IP means 0.0.0.0
		return false
	}
	nport, err := strconv.Atoi(port)
	if err != nil || nport <= 0 || nport > 65535 {
		// Invalid Port or port range
		return false
	}
	return true
}
