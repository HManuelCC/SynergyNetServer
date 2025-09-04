package comunication

import "fmt"

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

func (s *State) ToString() string {
	return fmt.Sprintf("Status: %v, Message: %s, Error: %s, Destination: %s, Origen: %s, LOCALPID: %d, SERVERPID: %d, Data: %v",
		s.Status, s.Message, s.Error, s.Destination, s.Origen, s.LOCALPID, s.SERVERPID, s.Data)
}

func (e *Event) ToString() string {
	return fmt.Sprintf("Event: %s, Destination: %s, Origen: %s, PID: %d, Data: %v",
		e.Event, e.Destination, e.Origen, e.PID, e.Data)
}
