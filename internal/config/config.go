package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/wb-go/wbf/config"
)

var Cfg = initConfig("config/config.yaml")

func initConfig(path string) *Config {
	wbfConfig := config.New()

	err := wbfConfig.Load(path)
	if err != nil {
		log.Fatal("could not read config file: ", err)
	}

	var cfg Config
	if err := wbfConfig.Unmarshal(&cfg); err != nil {
		log.Fatal("could not parse config file: ", err)
	}

	err = godotenv.Load(".env")
	if err != nil {
		log.Fatal("could not load .env file: ", err)
	}

	value, _ := os.LookupEnv("DB_PASSWORD")
	cfg.Postgres.Password = value

	return &cfg
}
