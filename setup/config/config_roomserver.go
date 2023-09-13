package config

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
)

type DataFrame struct {
	Matrix *Global `yaml:"-"`

	DefaultFrameVersion xtools.FrameVersion `yaml:"default_frame_version,omitempty"`

	Database DatabaseOptions `yaml:"database,omitempty"`
}

func (c *DataFrame) Defaults(opts DefaultOpts) {
	c.DefaultFrameVersion = xtools.FrameVersionV10
	if opts.Generate {
		if !opts.SingleDatabase {
			c.Database.ConnectionString = "file:dataframe.db"
		}
	}
}

func (c *DataFrame) Verify(configErrs *ConfigErrors) {
	if c.Matrix.DatabaseOptions.ConnectionString == "" {
		checkNotEmpty(configErrs, "frame_server.database.connection_string", string(c.Database.ConnectionString))
	}

	if !xtools.KnownFrameVersion(c.DefaultFrameVersion) {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'frame_server.default_frame_version': unsupported frame version: %q", c.DefaultFrameVersion))
	} else if !xtools.StableFrameVersion(c.DefaultFrameVersion) {
		log.Warnf("WARNING: Provided default frame version %q is unstable", c.DefaultFrameVersion)
	}
}
