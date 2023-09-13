package version

import (
	"fmt"

	"github.com/withqb/xtools"
)

// RoomVersions returns a map of all known room versions to this
// server.
func RoomVersions() map[xtools.RoomVersion]xtools.IRoomVersion {
	return xtools.RoomVersions()
}

// SupportedRoomVersions returns a map of descriptions for room
// versions that are supported by this homeserver.
func SupportedRoomVersions() map[xtools.RoomVersion]xtools.IRoomVersion {
	return xtools.RoomVersions()
}

// RoomVersion returns information about a specific room version.
// An UnknownVersionError is returned if the version is not known
// to the server.
func RoomVersion(version xtools.RoomVersion) (xtools.IRoomVersion, error) {
	if version, ok := xtools.RoomVersions()[version]; ok {
		return version, nil
	}
	return nil, UnknownVersionError{version}
}

// SupportedRoomVersion returns information about a specific room
// version. An UnknownVersionError is returned if the version is not
// known to the server, or an UnsupportedVersionError is returned if
// the version is known but specifically marked as unsupported.
func SupportedRoomVersion(version xtools.RoomVersion) (xtools.IRoomVersion, error) {
	return RoomVersion(version)
}

// UnknownVersionError is caused when the room version is not known.
type UnknownVersionError struct {
	Version xtools.RoomVersion
}

func (e UnknownVersionError) Error() string {
	return fmt.Sprintf("room version '%s' is not known", e.Version)
}

// UnsupportedVersionError is caused when the room version is specifically
// marked as unsupported.
type UnsupportedVersionError struct {
	Version xtools.RoomVersion
}

func (e UnsupportedVersionError) Error() string {
	return fmt.Sprintf("room version '%s' is marked as unsupported", e.Version)
}
