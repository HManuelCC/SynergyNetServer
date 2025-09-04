package balancer

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

type EventQueue struct {
	Tasks   []*providers.ProcessEvent
	Receive chan *providers.ProcessEvent
	Clients *client.ClientSliceGroups
	running bool
}

// Cola para eventos
var BalancerEventQueue *EventQueue = &EventQueue{Tasks: make([]*providers.ProcessEvent, 0), running: false}

func (q *EventQueue) GetTasks() []*providers.ProcessEvent {
	return q.Tasks
}

func (q *EventQueue) AddTask(p *providers.ProcessEvent) {
	p.PID = generateRandomPID(q)
	q.Receive <- p
}

func (q *EventQueue) RemoveTaskByPID(pid int) {
	for i, task := range q.Tasks {
		if task.GetPID() == pid {
			q.Tasks = append(q.Tasks[:i], q.Tasks[i+1:]...)
			break
		}
	}
}

func (q *EventQueue) GetTaskByPID(pid int) *providers.ProcessEvent {
	for _, task := range q.Tasks {
		if task.GetPID() == pid {
			return task
		}
	}
	return nil
}

func (q *EventQueue) GetTaskByIndex(index int) *providers.ProcessEvent {
	if index < 0 || index >= len(q.Tasks) {
		return nil
	}
	return q.Tasks[index]
}

func (q *EventQueue) Print() {
	fmt.Println("Estado actual de la cola de balanceo:")
	for i, task := range q.Tasks {
		fmt.Printf("Task %d: %+v\n", i, task)
	}
	fmt.Println("---------------------------------------------------")
}
func (q *EventQueue) Start(clients *client.ClientSliceGroups, buffer int, workers int) {
	q.Clients = clients
	q.Receive = make(chan *providers.ProcessEvent, buffer)
	for range workers {
		go func() {
			for proc := range q.Receive {
				q.Tasks = append(q.Tasks, proc)
				err := proc.ManageProccess(q.Clients)

				if proc.Attempts <= 0 {
					// Si no hay más intentos, se puede manejar el error
					proc.Attempts = 3
					q.RemoveTaskByPID(proc.GetPID())
					BalancerErrorEventQueue.AddTask(proc)
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

func (q *EventQueue) Stop() {
	close(q.Receive)
}

func (q *EventQueue) PopTask() *providers.ProcessEvent {
	if len(q.Tasks) == 0 {
		return nil
	}
	task := q.Tasks[0]
	q.Tasks = q.Tasks[1:]
	return task
}

func generateRandomPID(queue interface{}) int {
	rand.Seed(time.Now().UnixNano())
	pid := rand.Intn(10000)
	switch q := queue.(type) {
	case *EventQueue:
		// Verificamos que el PID sea único
		for _, existing := range q.Tasks {
			// Si encontramos un proceso con el mismo PID, generamos uno nuevo
			if existing.PID == pid {
				pid = generateRandomPID(q)
			}
		}
	case *StateQueue:
		// Verificamos que el PID sea único
		for _, existing := range q.Tasks {
			// Si encontramos un proceso con el mismo PID, generamos uno nuevo
			if existing.PID == pid {
				pid = generateRandomPID(q)
			}
		}
	}
	return pid

}
