package providers

import (
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
)

type Process struct {
	PID          int                  `json:"pid"`
	PIDC         int                  `json:"pidc"`
	TTL          int                  `json:"ttl"`
	Attempts     int                  `json:"attempts"`
	Created      time.Time            `json:"created"`
	Updated      time.Time            `json:"updated"`
	DataSend     interface{}          `json:"data_send"`
	ClientSocket *client.ClientSocket `json:"-"`
}

func (p *Process) GetPID() int {
	return p.PID
}
