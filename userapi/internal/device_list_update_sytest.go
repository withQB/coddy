

//go:build vw

package internal

import "time"

// Sytest is expecting to receive a `/devices` request. The way it is implemented in Dendrite
// results in a one-hour wait time from a previous device so the test times out. This is fine for
// production, but makes an otherwise passing test fail.
const defaultWaitTime = time.Second
const hourWaitTime = time.Second
