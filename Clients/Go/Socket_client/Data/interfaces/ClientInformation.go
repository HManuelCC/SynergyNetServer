package interfaces

import (
	"crypto/tls"
	"errors"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
)

type ClientHardwareResourcesStatistics struct {
	CPUUsage    float64
	MemoryUsage float64
	DiskUsage   float64
	DiskBusy    float64
}

type ClientInformation struct {
	ClientName string                            `json:"client_name"`
	Latency    float64                           `json:"latency"`
	Resources  ClientHardwareResourcesStatistics `json:"resources"`
	Events     EventsSubscribed                  `json:"events"`
}
type Client struct {
	host   string
	port   string
	name   string
	ApiKey *string

	// Conexión activa protegida por mutex de lectura/escritura.
	mu      sync.RWMutex
	Conn    net.Conn
	WriteMu sync.Mutex
	// Ciclo de vida
	closed  atomic.Bool
	done    chan struct{}
	attempt int

	// Estrategia de reconexión
	minBackoff time.Duration
	maxBackoff time.Duration

	// Config TLS (puedes exponer setter si luego quieres personalizarla)
	tlsConf *tls.Config
}

func NewClientHost(host, port, clientName string, apiKey *string, useTLS bool) *Client {
	return &Client{
		host:       host,
		port:       port,
		name:       clientName,
		ApiKey:     apiKey,
		done:       make(chan struct{}),
		attempt:    1,
		minBackoff: 1 * time.Second,
		maxBackoff: 30 * time.Second,
		tlsConf: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
}

func (c *ClientHardwareResourcesStatistics) GetSystemStats() error {
	// CPU (promedio en 1 segundo)
	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		return err
	}
	if len(cpuPercent) > 0 {
		c.CPUUsage = cpuPercent[0]
	}

	// Memoria
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		return err
	}
	c.MemoryUsage = vmStat.UsedPercent

	diskStat, err := disk.Usage("/")
	if err != nil {
		return err
	}
	c.DiskUsage = diskStat.UsedPercent

	ioStart, err := disk.IOCounters()
	if err != nil {
		return err
	}

	// Disco
	time.Sleep(1 * time.Second)

	ioEnd, err := disk.IOCounters()
	if err != nil {
		return err
	}

	// calcular diferencias (para el primer disco encontrado)
	for name, start := range ioStart {
		end := ioEnd[name]

		readDelta := end.ReadBytes - start.ReadBytes
		writeDelta := end.WriteBytes - start.WriteBytes

		// aquí solo como ejemplo: ocupación relativa (simplificada)
		totalIO := readDelta + writeDelta
		if totalIO > 0 {
			// asumimos que un disco puede mover ~100MB/s (depende del HW real)
			maxThroughput := float64(100 * 1024 * 1024) // 100 MB/s
			c.DiskBusy = (float64(totalIO) / maxThroughput) * 100
			if c.DiskBusy > 100 {
				c.DiskBusy = 100
			}
		}

		break // solo un disco para simplificar
	}
	return nil
}

func (c *Client) Send(event Event, timeout *time.Duration, cb ...ResponseCallback) error {
	if c.closed.Load() {
		return errors.New("client cerrado")
	}

	conn := c.GetConn()
	if conn == nil {
		return errors.New("no hay conexión activa")
	}

	// Serialize writes para evitar que dos goroutines mezclen paquetes.
	//<------ aqui esta el problema

	return event.SendData(c, timeout, cb...)
}

func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.done)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

// Connected indica si hay una conexión activa.
func (c *Client) Connected() bool {
	return c.GetConn() != nil
}

// -------------------- Internos --------------------

func (c *Client) SetConn(conn net.Conn) {
	c.mu.Lock()
	c.Conn = conn
	c.mu.Unlock()
}

func (c *Client) ClearConn() {
	c.mu.Lock()
	if c.Conn != nil {
		_ = c.Conn.Close()
	}
	c.Conn = nil
	c.mu.Unlock()
}

func (c *Client) GetConn() net.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Conn
}

func (c *Client) Run(useTLS bool, eventSlice *EventSlice) {
	for {
		if c.closed.Load() {
			return
		}

		start := time.Now()
		var conn net.Conn
		var err error
		if useTLS {
			conn, err = tls.Dial("tcp", net.JoinHostPort(c.host, c.port), c.tlsConf)
		} else {
			conn, err = net.Dial("tcp", net.JoinHostPort(c.host, c.port))
		}
		if err != nil {
			// Falló conectar: backoff y reintentar
			delay := c.backoff()
			log.Printf("[client %s] error al conectar: %v (reintento en %s, intento %d)", c.name, err, delay, c.attempt)
			select {
			case <-time.After(delay):
				c.attempt++
				continue
			case <-c.done:
				return
			}
		}

		// Reset de intentos al conectar
		c.attempt = 1

		handshakeLatency := time.Since(start).Milliseconds()
		log.Printf("[client %s] conectado (latencia handshake: %d ms)", c.name, handshakeLatency)

		// Publicar conexión
		c.SetConn(conn)

		// Lanzamos lector con el canal de estado del servidor
		serverStatus := make(chan bool, 1)
		go ReadData(c, c.name, eventSlice, serverStatus, float64(handshakeLatency))

		// Espera a desconexión o cierre del cliente
		select {
		case status := <-serverStatus:
			// ReadData manda false si se cae o hay error permanente
			if !status {
				log.Printf("[client %s] desconectado por el servidor, intentando reconectar...", c.name)
				c.ClearConn()
				// Loop sigue: vuelve a intentar conectar
				continue
			}
		case <-c.done:
			// Cierre explícito del cliente
			c.ClearConn()
			return
		}
	}
}

func (c *Client) backoff() time.Duration {
	// Exponencial con jitter
	// intento 1 => minBackoff, 2 => 2*min, 3 => 4*min, capped en maxBackoff
	base := c.minBackoff << (c.attempt - 1)
	if base > c.maxBackoff {
		base = c.maxBackoff
	}
	// Jitter +/- 20%
	jit := time.Duration(rand.Int63n(int64(base) / 5))
	if rand.Intn(2) == 0 {
		return base - jit
	}
	return base + jit
}
