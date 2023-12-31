package sqlutil

import (
	"github.com/lib/pq"
	"modernc.org/sqlite"
	lib "modernc.org/sqlite/lib"
)

// IsUniqueConstraintViolationErr returns true if the error is an unique_violation error
func IsUniqueConstraintViolationErr(err error) bool {
	switch e := err.(type) {
	case *pq.Error:
		return e.Code == "23505"
	case *sqlite.Error:
		return e.Code() == lib.SQLITE_CONSTRAINT
	}
	return false
}
