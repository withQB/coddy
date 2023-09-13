// Copyright 2017 Jan Christian Grünhage
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package synctypes

import (
	"errors"
)

// Filter is used by clients to specify how the server should filter responses to e.g. sync requests
type Filter struct {
	EventFields []string    `json:"event_fields,omitempty"`
	EventFormat string      `json:"event_format,omitempty"`
	Presence    EventFilter `json:"presence,omitempty"`
	AccountData EventFilter `json:"account_data,omitempty"`
	Frame        FrameFilter  `json:"frame,omitempty"`
}

// EventFilter is used to define filtering rules for events
type EventFilter struct {
	Limit      int       `json:"limit,omitempty"`
	NotSenders *[]string `json:"not_senders,omitempty"`
	NotTypes   *[]string `json:"not_types,omitempty"`
	Senders    *[]string `json:"senders,omitempty"`
	Types      *[]string `json:"types,omitempty"`
}

// FrameFilter is used to define filtering rules for frame-related events
type FrameFilter struct {
	NotFrames     *[]string       `json:"not_frames,omitempty"`
	Frames        *[]string       `json:"frames,omitempty"`
	Ephemeral    FrameEventFilter `json:"ephemeral,omitempty"`
	IncludeLeave bool            `json:"include_leave,omitempty"`
	State        StateFilter     `json:"state,omitempty"`
	Timeline     FrameEventFilter `json:"timeline,omitempty"`
	AccountData  FrameEventFilter `json:"account_data,omitempty"`
}

// StateFilter is used to define filtering rules for state events
type StateFilter struct {
	NotSenders                *[]string `json:"not_senders,omitempty"`
	NotTypes                  *[]string `json:"not_types,omitempty"`
	Senders                   *[]string `json:"senders,omitempty"`
	Types                     *[]string `json:"types,omitempty"`
	LazyLoadMembers           bool      `json:"lazy_load_members,omitempty"`
	IncludeRedundantMembers   bool      `json:"include_redundant_members,omitempty"`
	NotFrames                  *[]string `json:"not_frames,omitempty"`
	Frames                     *[]string `json:"frames,omitempty"`
	Limit                     int       `json:"limit,omitempty"`
	UnreadThreadNotifications bool      `json:"unread_thread_notifications,omitempty"`
	ContainsURL               *bool     `json:"contains_url,omitempty"`
}

// FrameEventFilter is used to define filtering rules for events in frames
type FrameEventFilter struct {
	Limit                     int       `json:"limit,omitempty"`
	NotSenders                *[]string `json:"not_senders,omitempty"`
	NotTypes                  *[]string `json:"not_types,omitempty"`
	Senders                   *[]string `json:"senders,omitempty"`
	Types                     *[]string `json:"types,omitempty"`
	LazyLoadMembers           bool      `json:"lazy_load_members,omitempty"`
	IncludeRedundantMembers   bool      `json:"include_redundant_members,omitempty"`
	NotFrames                  *[]string `json:"not_frames,omitempty"`
	Frames                     *[]string `json:"frames,omitempty"`
	UnreadThreadNotifications bool      `json:"unread_thread_notifications,omitempty"`
	ContainsURL               *bool     `json:"contains_url,omitempty"`
}

const (
	EventFormatClient     = "client"
	EventFormatFederation = "federation"
)

// Validate checks if the filter contains valid property values
func (filter *Filter) Validate() error {
	if filter.EventFormat != "" && filter.EventFormat != EventFormatClient && filter.EventFormat != EventFormatFederation {
		return errors.New("Bad event_format value. Must be one of [\"client\", \"federation\"]")
	}
	return nil
}

// DefaultFilter returns the default filter used by the Matrix server if no filter is provided in
// the request
func DefaultFilter() Filter {
	return Filter{
		AccountData: DefaultEventFilter(),
		EventFields: nil,
		EventFormat: "client",
		Presence:    DefaultEventFilter(),
		Frame: FrameFilter{
			AccountData:  DefaultFrameEventFilter(),
			Ephemeral:    DefaultFrameEventFilter(),
			IncludeLeave: false,
			NotFrames:     nil,
			Frames:        nil,
			State:        DefaultStateFilter(),
			Timeline:     DefaultFrameEventFilter(),
		},
	}
}

// DefaultEventFilter returns the default event filter used by the Matrix server if no filter is
// provided in the request
func DefaultEventFilter() EventFilter {
	return EventFilter{
		// parity with synapse: https://github.com/withqb/synapse/blob/v1.80.0/synapse/api/filtering.py#L336
		Limit:      10,
		NotSenders: nil,
		NotTypes:   nil,
		Senders:    nil,
		Types:      nil,
	}
}

// DefaultStateFilter returns the default state event filter used by the Matrix server if no filter
// is provided in the request
func DefaultStateFilter() StateFilter {
	return StateFilter{
		NotSenders:              nil,
		NotTypes:                nil,
		Senders:                 nil,
		Types:                   nil,
		LazyLoadMembers:         false,
		IncludeRedundantMembers: false,
		NotFrames:                nil,
		Frames:                   nil,
		ContainsURL:             nil,
	}
}

// DefaultFrameEventFilter returns the default frame event filter used by the Matrix server if no
// filter is provided in the request
func DefaultFrameEventFilter() FrameEventFilter {
	return FrameEventFilter{
		// parity with synapse: https://github.com/withqb/synapse/blob/v1.80.0/synapse/api/filtering.py#L336
		Limit:       10,
		NotSenders:  nil,
		NotTypes:    nil,
		Senders:     nil,
		Types:       nil,
		NotFrames:    nil,
		Frames:       nil,
		ContainsURL: nil,
	}
}
