package internal

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/withqb/xtools/spec"
	"github.com/withqb/xutil"
)

const (
	maxUsernameLength = 254 // http://matrix.org/speculator/spec/HEAD/intro.html#user-identifiers TODO account for domain

	minPasswordLength = 8   // http://matrix.org/docs/spec/client_server/r0.2.0.html#password-based
	maxPasswordLength = 512 // https://github.com/withqb/synapse/blob/v0.20.0/synapse/rest/client/v2_alpha/register.py#L161
)

var (
	ErrPasswordTooLong    = fmt.Errorf("password too long: max %d characters", maxPasswordLength)
	ErrPasswordWeak       = fmt.Errorf("password too weak: min %d characters", minPasswordLength)
	ErrUsernameTooLong    = fmt.Errorf("username exceeds the maximum length of %d characters", maxUsernameLength)
	ErrUsernameInvalid    = errors.New("username can only contain characters a-z, 0-9, or '_-./='")
	ErrUsernameUnderscore = errors.New("username cannot start with a '_'")
	validUsernameRegex    = regexp.MustCompile(`^[0-9a-z_\-=./]+$`)
)

// ValidatePassword returns an error if the password is invalid
func ValidatePassword(password string) error {
	// https://github.com/withqb/synapse/blob/v0.20.0/synapse/rest/client/v2_alpha/register.py#L161
	if len(password) > maxPasswordLength {
		return ErrPasswordTooLong
	} else if len(password) > 0 && len(password) < minPasswordLength {
		return ErrPasswordWeak
	}
	return nil
}

// PasswordResponse returns a xutil.JSONResponse for a given error, if any.
func PasswordResponse(err error) *xutil.JSONResponse {
	switch err {
	case ErrPasswordWeak:
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.WeakPassword(ErrPasswordWeak.Error()),
		}
	case ErrPasswordTooLong:
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(ErrPasswordTooLong.Error()),
		}
	}
	return nil
}

// ValidateUsername returns an error if the username is invalid
func ValidateUsername(localpart string, domain spec.ServerName) error {
	// https://github.com/withqb/synapse/blob/v0.20.0/synapse/rest/client/v2_alpha/register.py#L161
	if id := fmt.Sprintf("@%s:%s", localpart, domain); len(id) > maxUsernameLength {
		return ErrUsernameTooLong
	} else if !validUsernameRegex.MatchString(localpart) {
		return ErrUsernameInvalid
	} else if localpart[0] == '_' { // Regex checks its not a zero length string
		return ErrUsernameUnderscore
	}
	return nil
}

// UsernameResponse returns a xutil.JSONResponse for the given error, if any.
func UsernameResponse(err error) *xutil.JSONResponse {
	switch err {
	case ErrUsernameTooLong:
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	case ErrUsernameInvalid, ErrUsernameUnderscore:
		return &xutil.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidUsername(err.Error()),
		}
	}
	return nil
}

// ValidateApplicationServiceUsername returns an error if the username is invalid for an application service
func ValidateApplicationServiceUsername(localpart string, domain spec.ServerName) error {
	if id := fmt.Sprintf("@%s:%s", localpart, domain); len(id) > maxUsernameLength {
		return ErrUsernameTooLong
	} else if !validUsernameRegex.MatchString(localpart) {
		return ErrUsernameInvalid
	}
	return nil
}
