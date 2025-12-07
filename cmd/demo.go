package main

import (
	"os"
	"strconv"

	SynergyNetServer "github.com/HManuelCC/SynergyNetServer/Server"
)

func main() {

	//recibir puerto desde consola o variable de entorno
	var port int

	args := os.Args
	if len(args) > 1 {
		//convertir a int
		//si no es un numero, usar 443
		var err error
		port, err = strconv.Atoi(args[1])
		if err != nil {
			port = 443
		}
	} else {
		//revisar variable de entorno
		envPort := os.Getenv("SYNERGYNET_PORT")
		if envPort != "" {
			var err error
			port, err = strconv.Atoi(envPort)
			if err != nil {
				port = 443
			}
		}
	}

	SynergyNetServer.NewSocketServer(port, true)
}
