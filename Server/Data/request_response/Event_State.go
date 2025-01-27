package request_response

type Event struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}
type State struct {
	Status  bool        `json:"status"`
	Message string      `json:"state"`
	Error   string      `json:"error"`
	Data    interface{} `json:"data"`
}
