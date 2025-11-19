package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	SynergyNetClient "github.com/HManuelCC/SynergyNetClient/Socket_client"
)

func main() {
	//createEvents()

	testConn := SynergyNetClient.NewClient("localhost", "443", "test_go", nil, false)

	mux := http.NewServeMux()

	defer testConn.Close()

	go createRoutes(mux, testConn)

	http.ListenAndServe(":8080", mux)

	select {}
}

func createEvents() {
	SynergyNetClient.EventSlice.AddEvent("login", func(event SynergyNetClient.Event, conn *SynergyNetClient.Client, messagePid int, destination string) {

		var eventData SynergyNetClient.Event = SynergyNetClient.Event{
			Event: "registro",
			Data:  nil,
		}

		err := conn.Send(eventData, nil, func(response SynergyNetClient.State) {

			fmt.Println("Registro exitoso:", response.Message)

			response.SendData(conn, messagePid, destination)

		})

		if err != nil {
			fmt.Println("Error sending registro event:", err)
		}

	})

	SynergyNetClient.EventSlice.AddEvent("registro", func(event SynergyNetClient.Event, conn *SynergyNetClient.Client, messagePid int, destination string) {

		var state SynergyNetClient.State = SynergyNetClient.State{
			Status:  true,
			Message: "Hola amigo",
			Error:   "",
			Data:    nil,
		}

		fmt.Println("Mensaje recibido de registro: ", event.Origen, "Evento: ", event.Event)
		state.SendData(conn, messagePid, destination)

	})
}

func createRoutes(mux *http.ServeMux, testConn *SynergyNetClient.Client) {
	mux.HandleFunc("/login_prueba", func(w http.ResponseWriter, r *http.Request) {
		username := r.URL.Query().Get("username")
		event := SynergyNetClient.Event{
			Event: "login",
			Data: map[string]string{
				"username": username,
				"password": "test_password",
			},
		}
		fmt.Print("Enviando evento de login: ", event.Origen)

		err := testConn.Send(event, nil, func(response SynergyNetClient.State) {
			fmt.Println(response.Message)
			// Manejar la respuesta del evento aqu
			fmt.Println("Respuesta del evento de login:", username)
			json.NewEncoder(w).Encode(username)
		})

		if err != nil {
			fmt.Println("Error en respuesta:", err)
		}

	})
}
