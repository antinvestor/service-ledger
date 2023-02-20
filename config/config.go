package config

import "github.com/pitabwire/frame"

type LedgerConfig struct {
	frame.ConfigurationDefault

	PartitionServiceURI string `default:"127.0.0.1:7003" envconfig:"PARTITION_SERVICE_URI"`
}
