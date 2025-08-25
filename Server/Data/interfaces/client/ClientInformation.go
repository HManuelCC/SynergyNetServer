package client

import (
	"net"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
)

type ClientSocket struct {
	Host   string
	Port   string
	Conn   net.Conn
	Events chan comunication.Event
	States chan comunication.State
	Info   ClientInformation
}

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
}

type ClientSlice struct {
	group   string
	clients []*ClientSocket
}

type ClientSliceGroups []*ClientSlice
