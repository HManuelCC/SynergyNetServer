package balancer

import (
	"fmt"
	"strings"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

type ErrorStateQueue struct {
	Tasks   []*providers.ProcessState
	Receive chan *providers.ProcessState
	Clients *client.ClientSliceGroups
	running bool
}

var BalancerErrorStatesQueue *ErrorStateQueue = &ErrorStateQueue{Tasks: make([]*providers.ProcessState, 0), running: false}

func (q *ErrorStateQueue) GetTasks() []*providers.ProcessState {
	return q.Tasks
}

func (q *ErrorStateQueue) AddTask(p *providers.ProcessState) {
	p.PID = generateRandomPID(q)
	q.Receive <- p
}

func (q *ErrorStateQueue) RemoveTaskByPID(pid int) {
	for i, task := range q.Tasks {
		if task.GetPID() == pid {
			q.Tasks = append(q.Tasks[:i], q.Tasks[i+1:]...)
			break
		}
	}
}

func (q *ErrorStateQueue) GetTaskByPID(pid int) *providers.ProcessState {
	for _, task := range q.Tasks {
		if task.GetPID() == pid {
			return task
		}
	}
	return nil
}

func (q *ErrorStateQueue) GetTaskByIndex(index int) *providers.ProcessState {
	if index < 0 || index >= len(q.Tasks) {
		return nil
	}
	return q.Tasks[index]
}

func (q *ErrorStateQueue) Print() {
	fmt.Println("Estado actual de la cola de balanceo:")
	for i, task := range q.Tasks {
		fmt.Printf("Task %d: %+v\n", i, task)
	}
	fmt.Println("---------------------------------------------------")
}
func (q *ErrorStateQueue) Start(clients *client.ClientSliceGroups, buffer int, workers int) {
	q.Clients = clients
	q.Receive = make(chan *providers.ProcessState, buffer)
	for i := 0; i < workers; i++ {
		go func() {

			for proc := range q.Receive {
				q.Tasks = append(q.Tasks, proc)
				proc.ManageProccess(q.Clients)
				if proc.Attempts <= 0 {

					proc.Attempts = 3

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

func (q *ErrorStateQueue) Stop() {
	close(q.Receive)
}

func (q *ErrorStateQueue) PopTask() *providers.ProcessState {
	if len(q.Tasks) == 0 {
		return nil
	}
	task := q.Tasks[0]
	q.Tasks = q.Tasks[1:]
	return task
}
