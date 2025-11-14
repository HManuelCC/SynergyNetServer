package providers

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
)

type Process struct {
	ClientPID    int         `json:"client_pid"`
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

func (p *Process) ManageProcessByEventSubscription(clients *client.ClientSliceGroups, eventSubscriptions *client.ClientSliceGroupMapEventSubscription) error {

	//fmt.Println("Manejando events subscriptino")

	var clientDestination *client.ClientSocket = nil

	var err error = nil

	subscribers, exists := (*eventSubscriptions)[strings.ToUpper(p.DataSend.(comunication.Event).Event)]

	if !exists || len(subscribers.Subscribers) == 0 {
		log.Println("No subscribers found for event:", p.DataSend.(comunication.Event).Event)
		p.Updated = time.Now()
		return nil
	}

	if p.ClientPos >= len(subscribers.Subscribers) {
		p.ClientPos = 0
	}

	clientDestination = subscribers.Subscribers[p.ClientPos]

	if clientDestination == nil {
		log.Println("Subscriber client is nil for event:", p.DataSend.(comunication.Event).Event)
		p.Updated = time.Now()
		return errors.New("subscriber client is nil")
	}

	p.ClientSocket = clientDestination

	err = client.Emit(p.DataSend, p.ClientSocket, p.PID)

	p.Updated = time.Now()

	p.ClientPos = p.ClientPos + 1

	if err != nil {

		log.Println("Error al enviar el evento:", err)

		return err

	}

	return nil
}

func (proc *Process) ManageProccess(clients *client.ClientSliceGroups, eventsClientSubscripcion *client.ClientSliceGroupMapEventSubscription) error {

	var clientDestination *client.ClientSocket = nil

	var err error = nil

	switch proc.Type {
	case "EVENT":
		return proc.ManageProcessByEventSubscription(clients, eventsClientSubscripcion)
		//clientDestination, err = clients.SearchClientByNameGetClient(strings.ToUpper(proc.DataSend.(comunication.Event).Destination), proc.ClientPos)
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

			log.Println("Error al enviar el proceso:", err)

			return err

		}

		return nil

	}
}
