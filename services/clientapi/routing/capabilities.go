package routing

import (
	"net/http"

	dataframeAPI "github.com/withqb/coddy/services/dataframe/api"
	"github.com/withqb/coddy/services/dataframe/version"
	"github.com/withqb/xtools"
	"github.com/withqb/xutil"
)

// GetCapabilities returns information about the server's supported feature set
// and other relevant capabilities to an authenticated user.
func GetCapabilities(rsAPI dataframeAPI.ClientDataframeAPI) xutil.JSONResponse {
	versionsMap := map[xtools.FrameVersion]string{}
	for v, desc := range version.SupportedFrameVersions() {
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
			"m.frame_versions": map[string]interface{}{
				"default":   rsAPI.DefaultFrameVersion(),
				"available": versionsMap,
			},
		},
	}

	return xutil.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}
