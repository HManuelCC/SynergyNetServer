package database

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type ConnectionConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	Driver   string `json:"driver"`
}

func (e *ConnectionConfig) GetConnectionByEnv() error {
	envFile := ".env"

	// Verificar si existe el archivo .env
	if _, err := os.Stat(envFile); os.IsNotExist(err) {
		err := createEnvFile(envFile)
		return err
	}

	// Cargar variables desde .env
	err := godotenv.Load(envFile)
	if err != nil {
		log.Fatalf("Error cargando .env: %v", err)
	}

	// Leer variables
	e.Host = os.Getenv("host")
	e.Port = os.Getenv("port")
	e.Username = os.Getenv("username")
	e.Password = os.Getenv("password")
	e.Database = "SynergyNetDB"
	e.Driver = "mysql"

	if e.Host == "" || e.Port == "" || e.Username == "" || e.Password == "" {
		log.Fatal("Faltan variables de entorno en el archivo .env")
		return fmt.Errorf("missing environment variables")
	}

	fmt.Println("Variables cargadas:")
	fmt.Println("Host:", e.Host)
	fmt.Println("Port:", e.Port)
	fmt.Println("Username:", e.Username)
	fmt.Println("Password:", e.Password)

	return nil
}

func createEnvFile(envFile string) error {
	// Si no existe, crearlo con estructura base
	defaultEnv := `host=ip_server_address_or_domain_name
			port=port_number
			username=database_username
			password=database_password`

	err := os.WriteFile(envFile, []byte(defaultEnv), 0644)
	if err != nil {
		return err
	}
	fmt.Println("Archivo .env creado con estructura base.")
	return nil
}
