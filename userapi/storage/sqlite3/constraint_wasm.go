//go:build wasm
// +build wasm

package sqlite3

func isConstraintError(err error) bool {
	return false
}
