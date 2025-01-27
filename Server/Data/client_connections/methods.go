package client_socket

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/request_response"
)

type ClientSlice []*ClientSocket
type CallbackFunc func(result request_response.State)

func (c *ClientSocket) Connect() {
}

func (c *ClientSocket) Disconnect() {
}

func (c *ClientSocket) Send(event request_response.Event) {
	fmt.Println("Sending event to client: ", event.Event)
	c.Events <- event
}

func (c *ClientSocket) On(callback CallbackFunc) {
	callback(<-c.States)
}

func (c *ClientSlice) SearchClientByName(clientName string) (int, error) {
	for i := range *c {

		if (*c)[i].NameClient == clientName {

			return i, nil

		}
	}
	return 0, fmt.Errorf("client not found")
}

func (c *ClientSlice) SendDataToClientByClientName(clientName string, event request_response.Event) {
	index, _ := c.SearchClientByName(clientName)
	(*c)[index].Events <- event
}

func (c *ClientSlice) SendDataToClientByHost(host string, event request_response.Event) {
	index, _ := c.SearchClientByHost(host)
	(*c)[index].Events <- event
}

func (c *ClientSlice) SearchClientByNameGetClient(clientName string) (*ClientSocket, error) {
	for i := range *c {

		if (*c)[i].NameClient == clientName {

			return (*c)[i], nil

		}
	}
	return nil, fmt.Errorf("client not found")
}

func (c *ClientSlice) SearchClientByHost(host string) (int, error) {
	for i := range *c {

		if (*c)[i].Host == host {

			return i, nil

		}
	}
	return 0, fmt.Errorf("client not found")
}

func (c *ClientSlice) RemoveClient(host string) {
	index, _ := c.SearchClientByHost(host)
	*c = append((*c)[:index], (*c)[index+1:]...)
}

func (client *ClientSocket) SendData() {
	for event := range client.Events {
		data, err := json.Marshal(event)

		if err != nil {
			log.Println("Error al convertir el mensaje: ", err)
			return
		}

		messageBytes := []byte(string(data))

		messageSize := uint32(len(messageBytes))

		// Crear encabezado de 4 bytes con el tamaño del mensaje
		sizeBuffer := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuffer, messageSize)

		// Enviar tamaño y mensaje
		_, err = client.Conn.Write(sizeBuffer)
		if err != nil {
			fmt.Println("Error al enviar el tamaño:", err)
			return
		}

		fmt.Println("Enviando mensaje a cliente: ", string(data))

		client.Conn.Write([]byte(string(data)))
	}
}

func (client *ClientSocket) ReadData(connected chan bool) {

	for {
		sizeBuffer := make([]byte, 4)
		_, err := io.ReadFull(client.Conn, sizeBuffer)
		if err != nil {
			if err == io.EOF {
				log.Println("El cliente cerro la conexión")
				connected <- false
				return
			} else {
				fmt.Println("Error al leer el tamaño del mensaje:", err)
				break
			}

		}

		messageSize := binary.BigEndian.Uint32(sizeBuffer)

		message := make([]byte, messageSize)
		_, err = io.ReadFull(client.Conn, message)
		if err != nil {
			if err == io.EOF {
				log.Println("El cliente cerro la conexión")
				connected <- false
				return
			} else {
				log.Println("Error al leer el mensaje:", err)
				break
			}
		}

		state := request_response.State{}

		err = json.Unmarshal(message, &state)

		if err != nil {

			log.Println("Erro al obtener los datos:", err)
		} else {
			client.States <- state
		}

	}
}

func GetCLientName(conn net.Conn) (string, error) {
	event := request_response.Event{Event: "connect", Data: ""}
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

	state := request_response.State{}

	err = json.Unmarshal(message, &state)

	if err != nil {

		log.Println("Erro al obtener los datos:", err)
		return "", err
	}

	return state.Message, nil
}
