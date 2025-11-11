package interfaces

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"time"
)

type Process struct {
	PID       int              `json:"pid"`
	TTL       int              `json:"ttl"`
	Attempts  int              `json:"attempts"`
	Created   time.Time        `json:"created"`
	Updated   time.Time        `json:"updated"`
	Data      chan interface{} `json:"data"`
	OnTimeout func()           `json:"-"`
}

type ProcessSlice []*Process

var processes ProcessSlice

type ResponseCallback func(response State)

type Event struct {
	Event       string      `json:"event"`
	Destination string      `json:"destination"`
	Data        interface{} `json:"data"`
	Origen      string      `json:"origen"`
	PID         int         `json:"pid"`
}

type State struct {
	Status      bool        `json:"status"`
	Message     string      `json:"message"`
	Error       string      `json:"error"`
	Data        interface{} `json:"data"`
	Destination string      `json:"destination"`
	Origen      string      `json:"origen"`
	PID         int         `json:"pid"`
}

type MessageState struct {
	Status        bool   `json:"status"`
	ServerPID     int    `json:"server_pid"`
	Message       string `json:"state"`
	Error         string `json:"error"`
	ProcessStatus int    `json:"process_status"` // 0=pendiente, 1=en proceso, 2=finalizado
}

func (s State) ToString() string {
	return fmt.Sprintf("State{Status: %v, Message: %q, Error: %q, Data: %v, Destination: %q, Origen: %q, PID: %d}",
		s.Status, s.Message, s.Error, s.Data, s.Destination, s.Origen, s.PID)
}

func (m MessageState) ToString() string {
	return fmt.Sprintf("MessageState{Status: %v, ServerPID: %d, Message: %q, Error: %q}",
		m.Status, m.ServerPID, m.Message, m.Error)
}

func (object Event) SendData(client *Client, timeout *time.Duration, response ...ResponseCallback) error {

	var typeBuf byte = 1
	var pid int = generatePID()
	object.PID = pid

	// Convertir a JSON
	data, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("error al convertir el mensaje: %w", err)
	}

	// Tamaño del JSON
	messageSize := uint32(len(data))
	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, messageSize)

	// Armar el paquete completo: tipo (1B) + tamaño (4B) + datos JSON
	packet := append([]byte{typeBuf}, sizeBuffer...)
	packet = append(packet, data...)

	fmt.Println("intentando Escribir en el servidor")
	// Enviar
	client.WriteMu.Lock()
	_, err = client.Conn.Write(packet)
	client.WriteMu.Unlock()
	if err != nil {
		return fmt.Errorf("error al enviar el mensaje: %w", err)
	}

	fmt.Println("Escrito en el servidor")

	// Registrar proceso para esperar la respuesta
	process := &Process{
		PID:      pid,
		Data:     make(chan interface{}, 1), // bufferizado: no bloquea si ReadData responde rápido
		TTL:      3,
		Attempts: 2,
		Created:  time.Now(),
		Updated:  time.Now(),
	}
	processes = append(processes, process)

	// Esperar respuesta
	if len(response) > 0 {
		if timeout == nil {
			defaultTimeout := 15 * time.Second
			timeout = &defaultTimeout
		}
		select {
		case data := <-process.Data:
			callback := response[0]
			if state, ok := data.(State); ok && state.PID == pid {
				if state.Status {
					callback(state)
					return nil
				} else {
					return fmt.Errorf("error en la respuesta del servidor: %s", state.Error)
				}

			}

			return fmt.Errorf("se esperaba State pero se recibió otro tipo o PID incorrecto")

		case <-time.After(*timeout):
			//Eliminamos el proceso de la lista
			for i, process := range processes {
				if process.PID == pid {
					close(process.Data)                                   // Cerramos el canal para indicar que ya no se enviarán más
					processes = append(processes[:i], processes[i+1:]...) // Eliminamos el proceso de la lista
					log.Println("Proceso eliminado de la lista por que respondio y se encontro:", pid)
					return nil
				}
			}
			return fmt.Errorf("timeout esperando respuesta para PID %d", pid)
		}
	} else {
		go func() {
			select {
			case data := <-process.Data:
				if state, ok := data.(State); ok && state.PID == pid {
					log.Println("Respuesta recibida:", state)
				} else {
					log.Println("No se encontró el proceso o PID incorrecto para la respuesta recibida: ")
					//guardamos en log la respuesta para no perder la información
					log.Println("Respuesta recibida:", data)
				}
			case <-time.After(*timeout):
				log.Printf("Timeout esperando respuesta para PID %d", pid)
			}
		}()
	}
	return nil
}

func (object MessageState) SendData(client *Client) {
	var typeBuf byte = 3

	// Convertir a JSON
	data, err := json.Marshal(object)
	if err != nil {
		log.Println("Error al convertir el mensaje:", err)
		return
	}

	// Tamaño del JSON
	messageSize := uint32(len(data))
	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, messageSize)

	// Armar el paquete completo: tipo (1B) + tamaño (4B) + datos JSON
	packet := append([]byte{typeBuf}, sizeBuffer...)
	packet = append(packet, data...)

	// Un solo Write
	client.WriteMu.Lock()
	_, err = client.Conn.Write(packet)
	client.WriteMu.Unlock()
	if err != nil {
		log.Println("Error al enviar el mensaje:", err)
	}
	//fmt.Println("MensajeState enviado al servidor:", object.ToString())
}

func (object State) SendData(client *Client, messagePid int, destination string) {

	var stateResponse *MessageState = &MessageState{Message: "El servidor proceso la solicitud", Status: true, ServerPID: messagePid, Error: "", ProcessStatus: 2}
	stateResponse.SendData(client)

	object.Destination = destination

	var typeBuf byte = 2

	// Convertir a JSON
	data, err := json.Marshal(object)
	if err != nil {
		log.Println("Error al convertir el mensaje:", err)
		return
	}

	// Tamaño del JSON
	messageSize := uint32(len(data))
	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, messageSize)

	// Armar el paquete completo: tipo (1B) + tamaño (4B) + datos JSON
	packet := append([]byte{typeBuf}, sizeBuffer...)
	packet = append(packet, data...)

	// Un solo Write
	client.WriteMu.Lock()
	_, err = client.Conn.Write(packet)
	client.WriteMu.Unlock()
	if err != nil {
		log.Println("Error al enviar el mensaje:", err)
	}
}

func ReadData(conn *Client, clientName string, eventSlice *EventSlice, serverStatus chan bool, latency float64) {
	for {
		// Leer primero tipo + tamaño (9 bytes)
		header := make([]byte, 9)
		_, err := io.ReadFull(conn.Conn, header)
		if err != nil {
			if err == io.EOF {
				log.Println("El servidor cerró la conexión")
				serverStatus <- false
				conn.Close()
				return
			}

			netErr, ok := err.(net.Error)
			if ok && netErr.Temporary() {
				log.Println("Error al leer el encabezado:", err)
				var state *MessageState = &MessageState{Message: "El servidor no puede procesar la solicitud", Status: false, ServerPID: 0, Error: "", ProcessStatus: 2}
				state.SendData(conn)
				continue // 🔴 seguimos con el loop, no cerramos
			}

			// Errores permanentes: notificamos y terminamos
			log.Println("Error al leer encabezado, cerrando conexión:", err)
			serverStatus <- false
			conn.Close()
			return

		} else {

			// Primer byte = tipo
			msgType := header[0]
			// Siguientes 4 bytes = PID
			messagePidBuffer := binary.BigEndian.Uint32(header[1:5])
			//convertimos a int
			messagePid := int(messagePidBuffer)
			// Siguientes 4 bytes = tamaño
			messageSize := binary.BigEndian.Uint32(header[5:9])

			//fmt.Println("Server PID:", messagePid)

			// Leer el JSON completo
			data := make([]byte, messageSize)

			_, err = io.ReadFull(conn.Conn, data)
			if err != nil {
				log.Println("Error al leer el mensaje:", err)
				var state *MessageState = &MessageState{Message: "El servidor no puede procesar la solicitud", Status: false, ServerPID: messagePid, Error: err.Error(), ProcessStatus: 2}
				state.SendData(conn)
			} else {
				switch msgType {
				case 1: // Evento
					var event Event
					if err := json.Unmarshal(data, &event); err != nil {
						log.Println("Error al convertir el JSON:", err)
						break
					}
					//fmt.Println("Evento recibido:", event.Event)
					// lo mandamos al manejador de eventos
					var state *MessageState = &MessageState{Message: "El servidor proceso la solicitud", Status: true, ServerPID: messagePid, Error: "", ProcessStatus: 1}
					state.SendData(conn)
					HandleEvents(event, conn, clientName, eventSlice, int(latency), messagePid)
				case 2: // Estado
					var activeProcess bool = false
					var state State
					if err := json.Unmarshal(data, &state); err != nil {
						log.Println("Error al convertir el JSON:", err)
						break
					}
					var stateResponse *MessageState = &MessageState{Message: "El servidor proceso la solicitud", Status: true, ServerPID: messagePid, Error: "", ProcessStatus: 2}
					stateResponse.SendData(conn)
					found := false
					//fmt.Println("Estado recibido:", state.PID)
					for i, process := range processes {
						if process.PID == state.PID {
							found = true
							process.Data <- state
							close(process.Data)                                   // Cerramos el canal para indicar que ya no se enviarán más
							processes = append(processes[:i], processes[i+1:]...) // Eliminamos el proceso de la lista
							log.Println("Estado enviado al proceso con PID:", process.PID)
							activeProcess = true
							break
						}
					}
					if !found {
						log.Println("No se encontró un proceso con PID:", state.PID)
						log.Println("Respuesta recibida:", state.ToString())
					}
					if !activeProcess {
						// Escribimos en log
						log.Println("No se encontró un proceso activo para el estado:", state.ToString())
					}
				default:
					log.Println("Tipo de mensaje desconocido:", msgType)
				}
			}

		}

	}
}

func generatePID() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(1_000_000)
}
