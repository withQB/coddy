package types

import (
	"unsafe"

	"github.com/withqb/xtools"
	"github.com/withqb/xtools/spec"
)

// HeaderedEvent is an Event which serialises to the headered form, which includes
// _room_version and _event_id fields.
type HeaderedEvent struct {
	xtools.PDU
	Visibility xtools.HistoryVisibility
	// TODO: Remove this. This is a temporary workaround to store the userID in the syncAPI.
	// 		It really should be the userKey instead.
	UserID           spec.UserID
	StateKeyResolved *string
}

func (h *HeaderedEvent) CacheCost() int {
	return int(unsafe.Sizeof(*h)) +
		len(h.EventID()) +
		(cap(h.JSON()) * 2) +
		len(h.Version()) +
		1 // redacted bool
}

func (h *HeaderedEvent) MarshalJSON() ([]byte, error) {
	return h.PDU.ToHeaderedJSON()
}

func (j *HeaderedEvent) UnmarshalJSON(data []byte) error {
	ev, err := xtools.NewEventFromHeaderedJSON(data, false)
	if err != nil {
		return err
	}
	j.PDU = ev
	return nil
}

func NewEventJSONsFromHeaderedEvents(hes []*HeaderedEvent) xtools.EventJSONs {
	result := make(xtools.EventJSONs, len(hes))
	for i := range hes {
		result[i] = hes[i].JSON()
	}
	return result
}
