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
	PID         int         `json:"pid"`
	Data        interface{} `json:"data"`
}

type MessageState struct {
	Status        bool   `json:"status"`
	ServerPID     int    `json:"server_pid"`
	Message       string `json:"state"`
	Error         string `json:"error"`
	ProcessStatus int    `json:"process_status"` // 0=pendiente, 1=en proceso, 2=finalizado
}

func (s *State) ToString() string {
	return fmt.Sprintf("Status: %v, Message: %s, Error: %s, Destination: %s, Origen: %s, PID: %d, Data: %v",
		s.Status, s.Message, s.Error, s.Destination, s.Origen, s.PID, s.Data)
}

func (e *Event) ToString() string {
	return fmt.Sprintf("Event: %s, Destination: %s, Origen: %s, PID: %d, Data: %v",
		e.Event, e.Destination, e.Origen, e.PID, e.Data)
}

func (m *MessageState) ToString() string {
	return fmt.Sprintf("Status: %v, ServerPID: %d, Message: %s, Error: %s, ProcessStatus: %d",
		m.Status, m.ServerPID, m.Message, m.Error, m.ProcessStatus)
}
