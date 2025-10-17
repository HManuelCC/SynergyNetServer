package SynergyNetServer

import (
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/balancer"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
	"github.com/HManuelCC/SynergyNetServer/Server/handler_connections"
)

var Clients *client.ClientSliceGroups = &client.ClientSliceGroups{}

func NewSocketServer(port int) {

	// Inicializar el gestor de colas con TTL automático
	balancer.BalancerQueue.Start(Clients, 1000, 5)

	balancer.BalancerQueue.StartTTLManager()

	defer balancer.BalancerQueue.Stop()

	log.Println("SynergyNet Server iniciado con colas optimizadas...")

	startServer(port)

	log.Println("Servidor cerrado correctamente")
}

func startServer(port int) {
	var conexionesActuales int = 0

	/*cert, err := tls.LoadX509KeyPair("../Certs/server.crt", "../Certs/private_server.key")
	if err != nil {
		log.Fatal("Error cargando certificado:", err)
	}

	// Configuración TLS
	config := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS13}*/

	server, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Println("Error al crear el servidor: ", err)
		return
	}

	log.Println("Servidor corriendo en el puerto: ", port)

	for {
		conn, err := server.Accept()

		conexionesActuales++

		if err != nil {
			log.Println("Error al aceptar la conexión: ", err)
			continue
		} else {
			var addr string = conn.RemoteAddr().String()
			var port string = ""
			port = strings.Split(conn.RemoteAddr().String(), ":")[1]
			//fmt.Println("Obteniendo información del cliente")
			clientInfo, err := client.ConnectAndGetInfo(conn)
			if err != nil || clientInfo == nil {
				log.Println("Error al obtener la información del cliente: ", err)
				conn.Close()
			} else {
				clientInfo.ClientName = strings.ToUpper(clientInfo.ClientName)
				client := &client.ClientSocket{
					Info:          *clientInfo,
					Port:          port,
					Conn:          conn,
					Host:          addr,
					Events:        make(chan comunication.Event, 10),
					States:        make(chan comunication.State, 10),
					MessageStates: make(chan comunication.MessageState, 10),
				}
				Clients.AddClientToGroup(client)
				log.Println("Nueva conexión establecida: " + client.Info.ClientName)
				go handler_connections.HandleConnection(client, Clients)
			}

		}

	}
}
