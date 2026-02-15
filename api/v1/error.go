package apiv1

// ErrorResponse is the standard error envelope for all error responses.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}
