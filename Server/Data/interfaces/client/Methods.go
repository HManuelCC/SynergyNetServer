package client

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
)

var ClientErrors = struct {
	ErrClientNotFound   error
	ErrNoClientsInGroup error
	ErrClientOutOfRange error
}{
	ErrClientNotFound:   fmt.Errorf("client not found"),
	ErrNoClientsInGroup: fmt.Errorf("no clients in group"),
	ErrClientOutOfRange: fmt.Errorf("client out of range"),
}

type CallbackFunc func(result interface{})

func (c *ClientSocket) Connect() {
}

func (c *ClientSocket) Disconnect() {
}

func (c *ClientSocket) On(callback CallbackFunc) {
	for {
		select {
		case val, ok := <-c.Events:
			//fmt.Println("On Event received:", val, " ok:", ok)
			if !ok {
				c.Events = nil // evitar ciclo infinito al cerrarse
			} else {
				callback(val)
			}
		case val, ok := <-c.States:
			//fmt.Println("On State received:", val, " ok:", ok)
			if !ok {
				c.States = nil
			} else {
				callback(val)
			}
		case val, ok := <-c.MessageStates:
			//fmt.Println("On MessageState received:", val, " ok:", ok)
			if !ok {
				c.MessageStates = nil
			} else {
				callback(val)
			}
		}
		if c.Events == nil && c.States == nil && c.MessageStates == nil {
			return
		}
	}
}
func (c *ClientSliceGroups) SearchClientByName(clientName string) (int, error) {
	for i := range *c {

		if (*c)[i].group == strings.ToUpper(clientName) {

			if len((*c)[i].clients) > 0 {
				return i, nil
			} else {
				return 0, ClientErrors.ErrNoClientsInGroup
			}

		}
	}
	return 0, ClientErrors.ErrClientNotFound
}

func (c *ClientSliceGroups) SendDataToClientByClientName(clientName string, event comunication.Event) {
	index, _ := c.SearchClientByName(strings.ToUpper(clientName))
	(*c)[index].clients[0].Events <- event
}

func (c *ClientSliceGroups) SendDataToClientByHost(host string, event comunication.Event) error {
	indexGroup, indexClient, err := c.SearchClientByHost(host)
	if err != nil || indexGroup == -1 || indexClient == -1 {
		return err
	}
	(*c)[indexGroup].clients[indexClient].Events <- event
	return nil
}

func (c *ClientSliceGroups) SearchClientByNameGetClient(clientName string, pos int) (*ClientSocket, error) {
	for i := range *c {

		if (*c)[i].group == strings.ToUpper(clientName) {

			if len((*c)[i].clients) > 0 {
				if (pos + 1) > len((*c)[i].clients) {
					return nil, ClientErrors.ErrClientOutOfRange
				}
				return (*c)[i].clients[pos], nil
			} else {
				return nil, ClientErrors.ErrNoClientsInGroup
			}

		}
	}
	return nil, ClientErrors.ErrClientNotFound
}

func (c *ClientSliceGroups) SearchClientByHost(host string) (int, int, error) {
	for i := range *c {
		for j := range (*c)[i].clients {
			if (*c)[i].clients[j].Host == host {
				return i, j, nil
			}
		}
	}
	return -1, -1, fmt.Errorf("client not found")
}

func (c *ClientSliceGroups) RemoveClient(host string) {
	indexGroup, indexClient, _ := c.SearchClientByHost(host)
	if indexGroup == -1 || indexClient == -1 {
		return
	}

	(*c)[indexGroup].clients = append((*c)[indexGroup].clients[:indexClient], (*c)[indexGroup].clients[indexClient+1:]...)
	if len((*c)[indexGroup].clients) == 0 {
		*c = append((*c)[:indexGroup], (*c)[indexGroup+1:]...)
	}
	//c.Print
}

func (c *ClientSliceGroups) Print() error {
	if c == nil {
		return fmt.Errorf("client slice groups is nil")
	}
	for i := range *c {
		fmt.Printf("Grupo: %s\n", (*c)[i].group)
		for j := range (*c)[i].clients {
			fmt.Printf("  Cliente %d: %s\n", j, (*c)[i].clients[j].Info.ClientName)
		}
	}
	return nil
}

func (c *ClientSliceGroups) AddClientToGroup(client *ClientSocket) {
	for i := range *c {
		if (*c)[i].group == client.Info.ClientName {
			(*c)[i].clients = append((*c)[i].clients, client)
			SortClientsByLatency((*c)[i].clients)
			return
		}
	}
	newGroup := &ClientSlice{group: client.Info.ClientName, clients: []*ClientSocket{client}}
	*c = append(*c, newGroup)
}

func SortClientsByLatency(c []*ClientSocket) []*ClientSocket {
	sortedClients := make([]*ClientSocket, len(c))
	copy(sortedClients, c)

	for i := 0; i < len(sortedClients)-1; i++ {
		for j := 0; j < len(sortedClients)-i-1; j++ {
			if sortedClients[j].Info.Latency > sortedClients[j+1].Info.Latency {
				sortedClients[j], sortedClients[j+1] = sortedClients[j+1], sortedClients[j]
			}
		}
	}

	return sortedClients
}

func Emit(object interface{}, client *ClientSocket, pid int) error {

	var typeByte byte

	switch object.(type) {

	case comunication.Event:

		typeByte = 1

	case comunication.State:

		typeByte = 2

	default:

		log.Println("Tipo de evento no soportado:", object)

		return fmt.Errorf("tipo de evento no soportado: %T", object)
	}

	// Serializar a JSON
	data, err := json.Marshal(object)
	if err != nil {
		log.Println("Error al serializar el mensaje:", err)
		return err
	}

	// PID del proceso
	pidSend := make([]byte, 4)
	binary.BigEndian.PutUint32(pidSend, uint32(pid))

	// Tamaño del JSON
	messageSize := uint32(len(data))
	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, messageSize)

	// Construir paquete: [1 byte tipo] + [4 bytes PID] + [4 bytes tamaño] + [datos JSON]
	packet := append([]byte{typeByte}, pidSend...)
	packet = append(packet, sizeBuffer...)
	packet = append(packet, data...)

	// Un solo Write
	_, err = client.Conn.Write(packet)
	if err != nil {
		log.Println("Error al enviar el paquete:", err)
		return err
	}

	//fmt.Println("Mensaje enviado:", object)
	return nil
}

func (client *ClientSocket) ReadData(connected chan bool) {
	for {

		// Leer encabezado (1 byte tipo + 4 bytes tamaño)
		header := make([]byte, 5)
		_, err := io.ReadFull(client.Conn, header)
		if err != nil {
			if err == io.EOF {
				log.Println("El cliente cerró la conexión")
				connected <- false
				return
			}
			log.Println("Error al leer el encabezado:", err)
			break
		}

		tipo := header[0]
		messageSize := binary.BigEndian.Uint32(header[1:5])

		// Leer mensaje JSON completo
		message := make([]byte, messageSize)
		_, err = io.ReadFull(client.Conn, message)
		if err != nil {
			if err == io.EOF {
				log.Println("El cliente cerró la conexión")
				connected <- false
				return
			}
			log.Println("Error al leer el mensaje:", err)
			break
		}

		switch tipo {
		case 1: // Evento
			var event comunication.Event
			if err := json.Unmarshal(message, &event); err != nil {
				log.Println("Error al obtener los datos:", err)
			} else {

				client.Events <- event
			}

		case 2: // Estado
			var state comunication.State
			//fmt.Println("Estado recibido:", string(message))
			if err := json.Unmarshal(message, &state); err != nil {
				log.Println("Error al obtener los datos:", err)
			} else {
				client.States <- state
			}
		case 3: // MessageState
			var msgState comunication.MessageState
			//fmt.Println("MessageState recibido:", string(message))
			if err := json.Unmarshal(message, &msgState); err != nil {
				log.Println("Error al obtener los datos:", err)
			} else {
				//fmt.Println("Sending MessageState to channel:", msgState)
				client.MessageStates <- msgState
			}

		default:
			fmt.Println("Tipo desconocido:", tipo)
		}
	}
}

func ConnectAndGetInfo(conn net.Conn) (*ClientInformation, error) {
	event := comunication.Event{
		Event: "connect",
		Data:  "",
	}

	// Serializar
	data, err := json.Marshal(event)
	if err != nil {
		log.Println("Error al serializar el evento:", err)
		return nil, err
	}

	// Encabezado: tipo (1 byte = 1 para Event), tamaño (4 bytes)
	typeByte := byte(1) // Event
	pidSend := make([]byte, 4)

	binary.BigEndian.PutUint32(pidSend, uint32(0))
	sizeBuffer := make([]byte, 4)

	binary.BigEndian.PutUint32(sizeBuffer, uint32(len(data)))

	// Armar paquete: [tipo][PID][tamaño][payload]
	packet := append([]byte{typeByte}, pidSend...)
	packet = append(packet, sizeBuffer...)
	packet = append(packet, data...)

	// 2️⃣ Enviar con un solo Write
	if _, err := conn.Write(packet); err != nil {
		log.Println("Error al enviar el paquete:", err)
		return nil, err
	}

	//Recibimos principalmente si el evento pudo ser procesado
	//******__________________________________________________*******************

	// 3️⃣ Recibir encabezado de respuesta (1 byte tipo + 4 bytes tamaño)
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		log.Println("Error al leer encabezado:", err)
		return nil, err
	}

	respType := header[0]
	if respType != 3 { // Esperamos MessageState
		return nil, fmt.Errorf("tipo de respuesta inesperado: %d", respType)
	}

	messageSize := binary.BigEndian.Uint32(header[1:5])

	message := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, message); err != nil {
		log.Println("Error al leer el mensaje:", err)
		return nil, err
	}

	//fmt.Println("Mensaje recibido:", string(message))

	var messageState comunication.MessageState
	if err := json.Unmarshal(message, &messageState); err != nil {
		log.Println("Error al deserializar MessageState:", err)
		return nil, err
	}

	if !messageState.Status {
		log.Println("Error al procesar la solicitud:", messageState.Error)
		return nil, fmt.Errorf("error al procesar la solicitud: %s", messageState.Error)
	}

	header = make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		log.Println("Error al leer encabezado:", err)
		return nil, err
	}

	respType = header[0]
	if respType != 3 { // Esperamos MessageState
		return nil, fmt.Errorf("tipo de respuesta inesperado: %d", respType)
	}

	messageSize = binary.BigEndian.Uint32(header[1:5])

	message = make([]byte, messageSize)
	if _, err := io.ReadFull(conn, message); err != nil {
		log.Println("Error al leer el mensaje:", err)
		return nil, err
	}

	//fmt.Println("Mensaje recibido:", string(message))

	if err := json.Unmarshal(message, &messageState); err != nil {
		log.Println("Error al deserializar MessageState:", err)
		return nil, err
	}

	if !messageState.Status {
		log.Println("Error al procesar la solicitud:", messageState.Error)
		return nil, fmt.Errorf("error al procesar la solicitud: %s", messageState.Error)
	}

	//******__________________________________________________*******************

	//Despues recibimos la informacion de cliente

	// 3️⃣ Recibir encabezado de respuesta (1 byte tipo + 4 bytes tamaño)
	header = make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		log.Println("Error al leer encabezado:", err)
		return nil, err
	}

	respType = header[0]
	if respType != 2 { // Esperamos State
		return nil, fmt.Errorf("tipo de respuesta inesperado: %d", respType)
	}

	messageSize = binary.BigEndian.Uint32(header[1:5])

	message = make([]byte, messageSize)
	if _, err := io.ReadFull(conn, message); err != nil {
		log.Println("Error al leer el mensaje:", err)
		return nil, err
	}

	//fmt.Println("Mensaje recibido:", string(message))

	var stateInfo comunication.State
	if err := json.Unmarshal(message, &stateInfo); err != nil {
		log.Println("Error al deserializar State:", err)
		return nil, err
	}

	dataStr, ok := stateInfo.Data.(string)
	if !ok {
		//Si no es un string, inetntar decodificarlo en JSON desde Bytes
		var dataBytes []byte
		dataBytes, ok = stateInfo.Data.([]byte)
		if !ok {
			log.Println("state.Data no es un string ni un []byte")
			return nil, fmt.Errorf("state.Data no es un string ni un []byte")
		}
		dataStr = string(dataBytes)
	}

	// 6️⃣ `state.Data` es un string JSON con la info del cliente

	var clientInfo ClientInformation
	if err := json.Unmarshal([]byte(dataStr), &clientInfo); err != nil {
		log.Println("Error al deserializar ClientInformation:", err)
		return nil, err
	}

	return &clientInfo, nil
}

func GetClientName(conn net.Conn) (string, error) {
	event := comunication.Event{Event: "connect", Data: ""}
	data, err := json.Marshal(event)
	if err != nil {
		log.Println("Error al enviar el mensaje: ", err)
		return "", err
	}
	messageBytes := []byte(string(data))
	messageSize := uint32(len(messageBytes))

	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, messageSize)

	_, err = conn.Write(sizeBuffer)
	if err != nil {
		fmt.Println("Error al enviar el tamaño:", err)
	}
	conn.Write([]byte(string(data)))

	sizeBuffer = make([]byte, 4)
	_, err = io.ReadFull(conn, sizeBuffer)
	if err != nil {
		fmt.Println("Error al leer el tamaño del mensaje:", err)
		return "", err

	}

	messageSize = binary.BigEndian.Uint32(sizeBuffer)

	message := make([]byte, messageSize)
	_, err = io.ReadFull(conn, message)
	if err != nil {

		log.Println("Error al leer el mensaje:", err)
		return "", err

	}

	state := comunication.State{}

	err = json.Unmarshal(message, &state)

	if err != nil {

		log.Println("Erro al obtener los datos:", err)
		return "", err
	}

	return state.Message, nil
}
