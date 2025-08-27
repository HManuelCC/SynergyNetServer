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
	LOCALPID    int         `json:"local_pid"`
	SERVERPID   int         `json:"server_pid"`
	Data        interface{} `json:"data"`
}
