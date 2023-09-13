//go:build !wasm && cgo
// +build !wasm,cgo

package sqlutil

import (
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

// IsUniqueConstraintViolationErr returns true if the error is an unique_violation error
func IsUniqueConstraintViolationErr(err error) bool {
	switch e := err.(type) {
	case *pq.Error:
		return e.Code == "23505"
	case *sqlite3.Error:
		return e.Code == sqlite3.ErrConstraint
	case sqlite3.Error:
		return e.Code == sqlite3.ErrConstraint
	}
	return false
}
