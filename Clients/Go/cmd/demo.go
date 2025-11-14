package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	SynergyNetClient "github.com/HManuelCC/SynergyNetClient/Socket_client"
)

func main() {
	createEvents()

	testConn := SynergyNetClient.NewClient("localhost", "443", "test_go", nil, false)

	mux := http.NewServeMux()

	defer testConn.Close()

	go createRoutes(mux, testConn)

	http.ListenAndServe(":8080", mux)

	select {}
}

func createEvents() {
	SynergyNetClient.EventSlice.AddEvent("login", func(event SynergyNetClient.Event, conn *SynergyNetClient.Client, messagePid int, destination string) {

		var state SynergyNetClient.State = SynergyNetClient.State{
			Status:  true,
			Message: "Hola go",
			Error:   "",
			Data:    nil,
		}

		fmt.Println("Mensaje recibido: ", event.Origen)
		state.SendData(conn, messagePid, destination)

	})

	SynergyNetClient.EventSlice.AddEvent("registro", func(event SynergyNetClient.Event, conn *SynergyNetClient.Client, messagePid int, destination string) {

		var state SynergyNetClient.State = SynergyNetClient.State{
			Status:  true,
			Message: "Hola amigo",
			Error:   "",
			Data:    nil,
			PID:     event.PID,
		}

		fmt.Println("Mensaje recibido: ", event.Origen, "Evento: ", event.Event)
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

			fmt.Println(response.ToString())
			// Manejar la respuesta del evento aqu
			fmt.Println("Respuesta del evento de login:", username)
			json.NewEncoder(w).Encode(username)
		})

		if err != nil {
			fmt.Println("Error sending login event:", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})
}
