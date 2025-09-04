package balancer

import (
	"fmt"
	"strings"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

type ErrorEventQueue struct {
	Tasks   []*providers.ProcessEvent
	Receive chan *providers.ProcessEvent
	Clients *client.ClientSliceGroups
	running bool
}

var BalancerErrorEventQueue *ErrorEventQueue = &ErrorEventQueue{Tasks: make([]*providers.ProcessEvent, 0), running: false}

func (q *ErrorEventQueue) GetTasks() []*providers.ProcessEvent {
	return q.Tasks
}

func (q *ErrorEventQueue) AddTask(p *providers.ProcessEvent) {
	p.PID = generateRandomPID(q)
	q.Receive <- p
}

func (q *ErrorEventQueue) RemoveTaskByPID(pid int) {
	for i, task := range q.Tasks {
		if task.GetPID() == pid {
			q.Tasks = append(q.Tasks[:i], q.Tasks[i+1:]...)
			break
		}
	}
}

func (q *ErrorEventQueue) GetTaskByPID(pid int) *providers.ProcessEvent {
	for _, task := range q.Tasks {
		if task.GetPID() == pid {
			return task
		}
	}
	return nil
}

func (q *ErrorEventQueue) GetTaskByIndex(index int) *providers.ProcessEvent {
	if index < 0 || index >= len(q.Tasks) {
		return nil
	}
	return q.Tasks[index]
}

func (q *ErrorEventQueue) Print() {
	fmt.Println("Estado actual de la cola de balanceo:")
	for i, task := range q.Tasks {
		fmt.Printf("Task %d: %+v\n", i, task)
	}
	fmt.Println("---------------------------------------------------")
}
func (q *ErrorEventQueue) Start(clients *client.ClientSliceGroups, buffer int, workers int) {
	q.Clients = clients
	q.Receive = make(chan *providers.ProcessEvent, buffer)
	for i := 0; i < workers; i++ {
		go func() {
			for proc := range q.Receive {
				q.Tasks = append(q.Tasks, proc)
				proc.ManageProccess(q.Clients)
				if proc.Attempts <= 0 {
					// Si no hay más intentos, se puede manejar el error
					proc.Attempts = 3
					// Se implementara la logica de envio de error al producer
					var errorState comunication.State = comunication.State{
						Status:      false,
						Error:       "No se pudo enviar el estado al destino: informacion: {" + proc.DataSend.ToString() + "}",
						Message:     "Error en el procesamiento, del cliente",
						Destination: proc.DataSend.Destination,
						Origen:      proc.DataSend.Origen,
						LOCALPID:    proc.PIDC,
						SERVERPID:   proc.PID,
						Data:        proc.DataSend,
					}

					clientOrigen, err := clients.SearchClientByNameGetClient(strings.ToUpper(proc.DataSend.Origen), 0)

					if err != nil {

						return

					}

					if clientOrigen != nil {

						q.RemoveTaskByPID(proc.GetPID())

						client.Emit(errorState, clientOrigen)

					}

				} else {
					proc.Attempts = proc.Attempts - 1
					q.Receive <- proc
				}
			}
		}()
	}
}

func (q *ErrorEventQueue) Stop() {
	close(q.Receive)
}

func (q *ErrorEventQueue) PopTask() *providers.ProcessEvent {
	if len(q.Tasks) == 0 {
		return nil
	}
	task := q.Tasks[0]
	q.Tasks = q.Tasks[1:]
	return task
}
