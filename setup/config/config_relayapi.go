package config

type RelayAPI struct {
	Matrix *Global `yaml:"-"`

	// The database stores information used by the relay queue to
	// forward transactions to remote servers.
	Database DatabaseOptions `yaml:"database,omitempty"`
}

func (c *RelayAPI) Defaults(opts DefaultOpts) {
	if opts.Generate {
		if !opts.SingleDatabase {
			c.Database.ConnectionString = "file:relayapi.db"
		}
	}
}

func (c *RelayAPI) Verify(configErrs *ConfigErrors) {
	if c.Matrix.DatabaseOptions.ConnectionString == "" {
		checkNotEmpty(configErrs, "relay_api.database.connection_string", string(c.Database.ConnectionString))
	}
}
