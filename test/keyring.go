package test

import (
	"context"

	"github.com/withqb/xtools"
)

// NopJSONVerifier is a JSONVerifier that verifies nothing and returns no errors.
type NopJSONVerifier struct {
	// this verifier verifies nothing
}

func (t *NopJSONVerifier) VerifyJSONs(ctx context.Context, requests []xtools.VerifyJSONRequest) ([]xtools.VerifyJSONResult, error) {
	result := make([]xtools.VerifyJSONResult, len(requests))
	return result, nil
}
