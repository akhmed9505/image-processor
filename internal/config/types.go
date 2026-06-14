package config

type Config struct {
	Postgres   PostgresConfig   `mapstructure:"postgres"`
	HttpServer HttpServerConfig `mapstructure:"http_server"`
	Minio      MinioConfig      `mapstructure:"minio"`
	Kafka      KafkaConfig      `mapstructure:"kafka"`
}

type PostgresConfig struct {
	Name     string `mapstructure:"name"`
	User     string `mapstructure:"user"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
}

type HttpServerConfig struct {
	Address     string `mapstructure:"address"`
	Timeout     int    `mapstructure:"timeout"`
	IdleTimeout int    `mapstructure:"idle_timeout"`
}

type MinioConfig struct {
	Host string `mapstructure:"host"`
	Port string `mapstructure:"port"`
}

type KafkaConfig struct {
	Host string `mapstructure:"host"`
	Port string `mapstructure:"port"`
}
