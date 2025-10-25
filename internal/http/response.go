package http

type Status string

const (
	// StatusOK is used for health-check responses.
	StatusOK Status = "OK"

	// StatusSuccess indicates an operation completed successfully.
	StatusSuccess Status = "success"

	// StatusError indicates an operation failed.
	StatusError Status = "error"
)

// Response represents the standard API response format.
type Response struct {
	Status Status `json:"status,omitempty"`
	Value  string `json:"value,omitempty"`
	Error  string `json:"error,omitempty"`
}

func NewOKResponse() Response {
	return Response{Status: StatusOK}
}

func NewSuccessResponse() Response {
	return Response{Status: StatusSuccess}
}

func NewValueResponse(value string) Response {
	return Response{Status: StatusSuccess, Value: value}
}

func NewErrorResponse(err string) Response {
	return Response{Status: StatusError, Error: err}
}
