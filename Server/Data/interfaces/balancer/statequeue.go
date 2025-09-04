package balancer

import (
	"fmt"
	"log"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

type StateQueue struct {
	Tasks   []*providers.ProcessState
	Receive chan *providers.ProcessState
	Clients *client.ClientSliceGroups
	running bool
}

// Cola para estados
var BalancerStatesQueue *StateQueue = &StateQueue{Tasks: make([]*providers.ProcessState, 0), running: false}

func (q *StateQueue) GetTasks() []*providers.ProcessState {
	return q.Tasks
}

func (q *StateQueue) AddTask(p *providers.ProcessState) {
	p.PID = generateRandomPID(q)
	q.Receive <- p
}

func (q *StateQueue) RemoveTaskByPID(pid int) {
	for i, task := range q.Tasks {
		if task.GetPID() == pid {
			q.Tasks = append(q.Tasks[:i], q.Tasks[i+1:]...)
			break
		}
	}
}

func (q *StateQueue) GetTaskByPID(pid int) *providers.ProcessState {
	for _, task := range q.Tasks {
		if task.GetPID() == pid {
			return task
		}
	}
	return nil
}

func (q *StateQueue) GetTaskByIndex(index int) *providers.ProcessState {
	if index < 0 || index >= len(q.Tasks) {
		return nil
	}
	return q.Tasks[index]
}

func (q *StateQueue) Print() {
	fmt.Println("Estado actual de la cola de balanceo:")
	for i, task := range q.Tasks {
		fmt.Printf("Task %d: %+v\n", i, task)
	}
	fmt.Println("---------------------------------------------------")
}
func (q *StateQueue) Start(clients *client.ClientSliceGroups, buffer int, workers int) {
	q.Clients = clients
	q.Receive = make(chan *providers.ProcessState, buffer)
	for i := 0; i < workers; i++ {
		go func() {

			for proc := range q.Receive {
				q.Tasks = append(q.Tasks, proc)
				err := proc.ManageProccess(q.Clients)

				if proc.Attempts <= 0 {
					// Si no hay más intentos, se puede manejar el error
					proc.Attempts = 3
					q.RemoveTaskByPID(proc.GetPID())
					BalancerErrorStatesQueue.AddTask(proc)
				}

				if err != nil {
					log.Println("Error al gestionar el proceso:", err)
					proc.Attempts = proc.Attempts - 1
					q.Receive <- proc
				}

			}

		}()
	}
}

func (q *StateQueue) Stop() {
	close(q.Receive)
}

func (q *StateQueue) PopTask() *providers.ProcessState {
	if len(q.Tasks) == 0 {
		return nil
	}
	task := q.Tasks[0]
	q.Tasks = q.Tasks[1:]
	return task
}
