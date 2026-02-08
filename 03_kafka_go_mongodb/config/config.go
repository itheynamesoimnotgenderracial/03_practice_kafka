package config

import (
	"os"

	"github.com/spf13/viper"
)

// Config stores all configuration of the application
// The values are read by viper from a config file or environment variables.
type Config struct {
	KafkaBrokerHost string `mapstructure:"KAFKA_BROKER_HOST"`
	KafkaTopic      string `mapstructure:"KAFKA_TOPIC"`
	KafkaGroupId    string `mapstructure:"KAFKA_GROUP_ID"`
	MongodbDatabase string `mapstructure:"MONGODB_DATABASE"`
	MongodbHostName string `mapstructure:"MONGODB_HOST_NAME"`
	MongodbPort     int    `mapstructure:"MONGODB_PORT"`
}

func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viperFileConfiguration("dev")

	err = viper.ReadInConfig()
	if err != nil {
		viperFileConfiguration("app")
	}

	err = viper.Unmarshal(&config)

	return
}

func viperFileConfiguration(filename string) {
	viper.SetConfigName(filename)
	viper.SetConfigType("env")
	viper.AutomaticEnv()
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
