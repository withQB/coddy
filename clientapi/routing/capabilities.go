package routing

import (
	"net/http"

	roomserverAPI "github.com/withqb/coddy/roomserver/api"
	"github.com/withqb/coddy/roomserver/version"
	"github.com/withqb/xtools"
	"github.com/withqb/xutil"
)

// GetCapabilities returns information about the server's supported feature set
// and other relevant capabilities to an authenticated user.
func GetCapabilities(rsAPI roomserverAPI.ClientRoomserverAPI) xutil.JSONResponse {
	versionsMap := map[xtools.RoomVersion]string{}
	for v, desc := range version.SupportedRoomVersions() {
		if desc.Stable() {
			versionsMap[v] = "stable"
		} else {
			versionsMap[v] = "unstable"
		}
	}

	response := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"m.change_password": map[string]bool{
				"enabled": true,
			},
			"m.room_versions": map[string]interface{}{
				"default":   rsAPI.DefaultRoomVersion(),
				"available": versionsMap,
			},
		},
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}
