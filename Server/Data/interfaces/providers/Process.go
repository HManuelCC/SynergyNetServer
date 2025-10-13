package providers

import (
	"log"
	"strings"
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
)

type Process struct {
	PID          int         `json:"pid"`
	TTL          int         `json:"ttl"`
	Attempts     int         `json:"attempts"`
	Created      time.Time   `json:"created"`
	Updated      time.Time   `json:"updated"`
	DataSend     interface{} `json:"data_send"`
	ClientPos    int
	ClientSocket *client.ClientSocket `json:"-"`
	Type         string               `json:"type"` // "event" o "state"
	Priority     int                  `json:"priority"`
}

func (p *Process) GetPID() int {
	return p.PID
}

func (proc *Process) ManageProccess(clients *client.ClientSliceGroups) error {

	var clientDestination *client.ClientSocket = nil

	var err error = nil

	switch proc.Type {
	case "EVENT":
		clientDestination, err = clients.SearchClientByNameGetClient(strings.ToUpper(proc.DataSend.(comunication.Event).Destination), proc.ClientPos)
	case "STATE":
		clientDestination, err = clients.SearchClientByNameGetClient(strings.ToUpper(proc.DataSend.(comunication.State).Destination), proc.ClientPos)
	default:
		log.Println("Tipo de proceso desconocido:", proc.Type)
		return nil
	}

	if err == client.ClientErrors.ErrClientOutOfRange {

		proc.ClientPos = 0

		switch proc.Type {
		case "EVENT":
			clientDestination, err = clients.SearchClientByNameGetClient(strings.ToUpper(proc.DataSend.(comunication.Event).Destination), proc.ClientPos)
		case "STATE":
			clientDestination, err = clients.SearchClientByNameGetClient(strings.ToUpper(proc.DataSend.(comunication.State).Destination), proc.ClientPos)
		default:
			log.Println("Tipo de proceso desconocido:", proc.Type)
			return nil
		}

	}

	if err != nil || clientDestination == nil {

		proc.ClientPos = 0

		proc.Updated = time.Now()

		return err

	} else {

		proc.ClientSocket = clientDestination

		err = client.Emit(proc.DataSend, proc.ClientSocket, proc.PID)

		proc.Updated = time.Now()

		proc.ClientPos = proc.ClientPos + 1

		if err != nil {

			log.Println("Error al enviar el evento:", err)

			return err

		}

		return nil

	}
}
