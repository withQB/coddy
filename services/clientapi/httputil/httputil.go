package httputil

import (
	"encoding/json"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

// UnmarshalJSONRequest into the given interface pointer. Returns an error JSON response if
// there was a problem unmarshalling. Calling this function consumes the request body.
func UnmarshalJSONRequest(req *http.Request, iface interface{}) *xutil.JSONResponse {
	// encoding/json allows invalid utf-8, coddy does not
	body, err := io.ReadAll(req.Body)
	if err != nil {
		xutil.GetLogger(req.Context()).WithError(err).Error("io.ReadAll failed")
		return &xutil.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return UnmarshalJSON(body, iface)
}

func UnmarshalJSON(body []byte, iface interface{}) *xutil.JSONResponse {
	if !utf8.Valid(body) {
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("Body contains invalid UTF-8"),
		}
	}

	if err := json.Unmarshal(body, iface); err != nil {
		// TDO: We may want to suppress the Error() return in production? It's useful when
		// debugging because an error will be produced for both invalid/malformed JSON AND
		// valid JSON with incorrect types for values.
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}
	return nil
}
