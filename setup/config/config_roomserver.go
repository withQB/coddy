package config

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/withqb/xtools"
)

type RoomServer struct {
	Matrix *Global `yaml:"-"`

	DefaultRoomVersion xtools.RoomVersion `yaml:"default_room_version,omitempty"`

	Database DatabaseOptions `yaml:"database,omitempty"`
}

func (c *RoomServer) Defaults(opts DefaultOpts) {
	c.DefaultRoomVersion = xtools.RoomVersionV10
	if opts.Generate {
		if !opts.SingleDatabase {
			c.Database.ConnectionString = "file:roomserver.db"
		}
	}
}

func (c *RoomServer) Verify(configErrs *ConfigErrors) {
	if c.Matrix.DatabaseOptions.ConnectionString == "" {
		checkNotEmpty(configErrs, "room_server.database.connection_string", string(c.Database.ConnectionString))
	}

	if !xtools.KnownRoomVersion(c.DefaultRoomVersion) {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'room_server.default_room_version': unsupported room version: %q", c.DefaultRoomVersion))
	} else if !xtools.StableRoomVersion(c.DefaultRoomVersion) {
		log.Warnf("WARNING: Provided default room version %q is unstable", c.DefaultRoomVersion)
	}
}
