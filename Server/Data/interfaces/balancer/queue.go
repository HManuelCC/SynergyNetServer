package balancer

import (
	"log"
	"math/rand"
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

type Queue struct {
	Tasks   []*providers.Process
	Receive chan *providers.Process
	End     chan *providers.Process
	running bool
}

var BalancerQueue *Queue = &Queue{Tasks: make([]*providers.Process, 0),
	Receive: make(chan *providers.Process), End: make(chan *providers.Process), running: false}

var ReintentQueue *Queue = &Queue{Tasks: make([]*providers.Process, 0),
	Receive: make(chan *providers.Process), End: make(chan *providers.Process), running: false}

func (q *Queue) GetTasks() []*providers.Process {
	return q.Tasks
}

func (q *Queue) AddTask(p *providers.Process) {
	p.PID = generateRandomPID()
	q.Receive <- p
}

func (q *Queue) RemoveTaskByPID(pid int) {
	for i, task := range q.Tasks {
		if task.GetPID() == pid {
			q.Tasks = append(q.Tasks[:i], q.Tasks[i+1:]...)
			break
		}
	}
}

func (q *Queue) GetTaskByPID(pid int) *providers.Process {
	for _, task := range q.Tasks {
		if task.GetPID() == pid {
			return task
		}
	}
	return nil
}

func (q *Queue) Start() {
	go func() {

		for proc := range q.Receive {

			q.Tasks = append(q.Tasks, proc)
			var err error = nil

			switch proc.DataSend.(type) {
			case comunication.Event:
				var dataEvent comunication.Event = proc.DataSend.(comunication.Event)
				dataEvent.PID = proc.PID
				err = client.Emit(dataEvent, proc.ClientSocket)
			case comunication.State:
				var dataState comunication.State = proc.DataSend.(comunication.State)
				dataState.PID = proc.PIDC
				err = client.Emit(dataState, proc.ClientSocket)
			default:
				log.Println("Tipo de evento no soportado:", proc.DataSend)
				return
			}

			if err != nil {

				log.Println("Error al enviar el evento:", err)

				//ReintentQueue.AddTask(proc)

			} else {

				proc.Updated = time.Now()

				proc.Attempts = proc.Attempts - 1

			}
		}

	}()
}

func (q *Queue) ManageEnd() {
	for proc := range q.End {
		q.RemoveTaskByPID(proc.GetPID())
	}
}

func (q *Queue) Stop() {
	close(q.Receive)
	close(q.End)
}

func (q *Queue) PopTask() *providers.Process {
	if len(q.Tasks) == 0 {
		return nil
	}
	task := q.Tasks[0]
	q.Tasks = q.Tasks[1:]
	return task
}

func generateRandomPID() int {
	rand.Seed(time.Now().UnixNano())
	pid := rand.Intn(10000)

	// Verificamos que el PID sea único
	for _, existing := range BalancerQueue.GetTasks() {
		// Si encontramos un proceso con el mismo PID, generamos uno nuevo
		if existing.PID == pid {
			pid = generateRandomPID()
			// Reiniciamos la búsqueda
			existing = nil
		}
	}
	return pid
}
