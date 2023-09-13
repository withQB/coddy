package version

import (
	"fmt"

	"github.com/withqb/xtools"
)

// FrameVersions returns a map of all known frame versions to this
// server.
func FrameVersions() map[xtools.FrameVersion]xtools.IFrameVersion {
	return xtools.FrameVersions()
}

// SupportedFrameVersions returns a map of descriptions for frame
// versions that are supported by this homeserver.
func SupportedFrameVersions() map[xtools.FrameVersion]xtools.IFrameVersion {
	return xtools.FrameVersions()
}

// FrameVersion returns information about a specific frame version.
// An UnknownVersionError is returned if the version is not known
// to the server.
func FrameVersion(version xtools.FrameVersion) (xtools.IFrameVersion, error) {
	if version, ok := xtools.FrameVersions()[version]; ok {
		return version, nil
	}
	return nil, UnknownVersionError{version}
}

// SupportedFrameVersion returns information about a specific frame
// version. An UnknownVersionError is returned if the version is not
// known to the server, or an UnsupportedVersionError is returned if
// the version is known but specifically marked as unsupported.
func SupportedFrameVersion(version xtools.FrameVersion) (xtools.IFrameVersion, error) {
	return FrameVersion(version)
}

// UnknownVersionError is caused when the frame version is not known.
type UnknownVersionError struct {
	Version xtools.FrameVersion
}

func (e UnknownVersionError) Error() string {
	return fmt.Sprintf("frame version '%s' is not known", e.Version)
}

// UnsupportedVersionError is caused when the frame version is specifically
// marked as unsupported.
type UnsupportedVersionError struct {
	Version xtools.FrameVersion
}

func (e UnsupportedVersionError) Error() string {
	return fmt.Sprintf("frame version '%s' is marked as unsupported", e.Version)
}
