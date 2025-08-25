package comunication

type Event struct {
	Event       string      `json:"event"`
	Destination string      `json:"destination"`
	Origen      string      `json:"origen"`
	PID         int         `json:"pid"`
	Data        interface{} `json:"data"`
}

type State struct {
	Status      bool        `json:"status"`
	Message     string      `json:"state"`
	Error       string      `json:"error"`
	Destination string      `json:"destination"`
	Origen      string      `json:"origen"`
	PID         int         `json:"pid"`
	Data        interface{} `json:"data"`
}
