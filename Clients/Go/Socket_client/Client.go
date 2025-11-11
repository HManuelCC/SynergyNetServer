package SynergyNetClient

import (
	"github.com/HManuelCC/SynergyNetClient/Socket_client/Data/interfaces"
)

// Reutiliza tu EventSlice global
var EventSlice *interfaces.EventSlice = &interfaces.EventSlice{}

// Client maneja la vida de la conexión, reconexiones y envío seguro.

// NewClient crea el cliente y arranca el bucle de conexión/reconexión.
func NewClient(host, port, clientName string, apiKey *string, useTLS bool) *interfaces.Client {
	c := interfaces.NewClientHost(host, port, clientName, apiKey, useTLS)

	go c.Run(useTLS, EventSlice)
	return c
}

// Send envía un Event usando la conexión actual y maneja el callback/timeout
// exactamente como tu interfaces.Event.SendData, pero asegurando write serializado.

// Close cierra el cliente y la conexión actual.
