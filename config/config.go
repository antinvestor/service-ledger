package config

import "github.com/pitabwire/frame"

type LedgerConfig struct {
	frame.ConfigurationDefault

	SecurelyRunService bool `default:"true" envconfig:"SECURELY_RUN_SERVICE"`
}
