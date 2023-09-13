//go:build wasm
// +build wasm

package sqlutil

import (
	"modernc.org/sqlite"
	lib "modernc.org/sqlite/lib"
)

// IsUniqueConstraintViolationErr returns true if the error is an unique_violation error
func IsUniqueConstraintViolationErr(err error) bool {
	switch e := err.(type) {
	case *sqlite.Error:
		return e.Code() == lib.SQLITE_CONSTRAINT
	}
	return false
}
