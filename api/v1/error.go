package apiv1

import "fmt"

// ErrorResponse is the standard error envelope for all error responses.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// Error is a domain error that service methods return to signal a specific
// HTTP status and error code to the handler.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// Common error constructors.

func ErrBadRequest(msg string) *Error {
	return &Error{Status: 400, Code: "VALIDATION_ERROR", Message: msg}
}

func ErrNotFound(msg string) *Error {
	return &Error{Status: 404, Code: "NOT_FOUND", Message: msg}
}

func ErrConflict(msg string) *Error {
	return &Error{Status: 409, Code: "CONFLICT", Message: msg}
}

func ErrInternal(err error) *Error {
	return &Error{Status: 500, Code: "INTERNAL_ERROR", Message: fmt.Sprintf("internal error: %v", err)}
}
