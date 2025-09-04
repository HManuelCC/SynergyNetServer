package providers

import (
	"log"
	"strings"
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
)

type ProcessEvent struct {
	PID          int                `json:"pid"`
	PIDC         int                `json:"pidc"`
	TTL          int                `json:"ttl"`
	Attempts     int                `json:"attempts"`
	Created      time.Time          `json:"created"`
	Updated      time.Time          `json:"updated"`
	DataSend     comunication.Event `json:"data_send"`
	ClientPos    int
	ClientSocket *client.ClientSocket `json:"-"`
}

type ProcessState struct {
	PID          int                `json:"pid"`
	PIDC         int                `json:"pidc"`
	TTL          int                `json:"ttl"`
	Attempts     int                `json:"attempts"`
	Created      time.Time          `json:"created"`
	Updated      time.Time          `json:"updated"`
	DataSend     comunication.State `json:"data_send"`
	ClientPos    int
	ClientSocket *client.ClientSocket `json:"-"`
}

func (p *ProcessEvent) GetPID() int {
	return p.PID
}

func (p *ProcessState) GetPID() int {
	return p.PID
}

func (proc *ProcessEvent) ManageProccess(clients *client.ClientSliceGroups) error {
	clientDestination, err := clients.SearchClientByNameGetClient(strings.ToUpper(proc.DataSend.Destination), proc.ClientPos)

	if err != nil || clientDestination == nil {

		proc.ClientPos = 0

		return err

	} else {

		proc.ClientSocket = clientDestination

		proc.DataSend.PID = proc.PID

		err = client.Emit(proc.DataSend, proc.ClientSocket)

		proc.Updated = time.Now()

		proc.ClientPos = proc.ClientPos + 1

		if err != nil {

			log.Println("Error al enviar el evento:", err)

			err := proc.ManageProccess(clients)

			if err != nil {
				return err
			}

		}

	}

	return nil
}

func (proc *ProcessState) ManageProccess(clients *client.ClientSliceGroups) error {
	clientDestination, err := clients.SearchClientByNameGetClient(strings.ToUpper(proc.DataSend.Destination), proc.ClientPos)

	if err != nil || clientDestination == nil {

		proc.ClientPos = 0

		return err

	} else {

		proc.ClientSocket = clientDestination

		proc.DataSend.SERVERPID = proc.PID

		proc.DataSend.LOCALPID = proc.PIDC

		err = client.Emit(proc.DataSend, proc.ClientSocket)

		proc.Updated = time.Now()

		proc.ClientPos = proc.ClientPos + 1

		if err != nil {

			log.Println("Error al enviar el evento:", err)

			err := proc.ManageProccess(clients)

			if err != nil {
				return err
			}

		}

		return nil

	}
}
