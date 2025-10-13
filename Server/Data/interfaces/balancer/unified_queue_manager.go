package balancer

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/comunication"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

// Errores personalizados
var (
	ErrInvalidPID      = errors.New("invalid PID: must be greater than 0")
	ErrProcessNotFound = errors.New("process not found or already handled")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrProcessFailed   = errors.New("process failed after multiple attempts: ")
)

type QueueManager interface {
	AddTask(t providers.Process)
	RemoveTaskByPID(pid int)
	GetTaskByPID(pid int) *providers.Process
	GetTasks() map[int]*providers.Process
	Print()
	Start(clients *client.ClientSliceGroups, buffer int, workers int)
	Stop()
	ManageProcessWithMessageState(state comunication.MessageState, clientSocket *client.ClientSocket) error
	StartTTLManager()
}

func (utm *Queue) ManageProcessWithMessageState(state comunication.MessageState, clientSocket *client.ClientSocket) error {
	if state.ServerPID <= 0 {
		return ErrInvalidPID
	}

	task := utm.GetTaskByPID(state.ServerPID)
	if task == nil {
		return ErrProcessNotFound
	}

	if state.Status { // 2 = Proceso completado exitosamente
		// Proceso exitoso, eliminar de la cola
		if state.ProcessStatus == 2 {
			utm.RemoveTaskByPID(state.ServerPID)
			return nil
		} else {
			// Si el proceso no está completo, simplemente actualizar el estado
			task.Updated = time.Now()
			return nil
		}

	} else {
		// Proceso fallido, manejar reintentos o errores
		task.Attempts--
		if task.Attempts <= 0 {
			// Aquí podrías agregar lógica para manejar procesos que han fallado múltiples veces
			utm.RemoveTaskByPID(state.ServerPID)
			return fmt.Errorf("%w%s", ErrProcessFailed, state.Error)
		}
	}

	return nil
}

func (q *Queue) StartTTLManager() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			q.mutex.Lock()
			for pid, task := range q.Tasks {
				elapsed := time.Since(task.Updated)
				if elapsed.Seconds() >= float64(task.TTL) {
					task.Attempts--
					task.Priority += 1
					task.Updated = time.Now()

					if task.Attempts == 0 {
						fmt.Printf("[TTL] PID %d falló tras %d intentos, eliminando.\n", pid, task.Attempts)
						delete(q.Tasks, pid)
						continue
					}

					fmt.Printf("[TTL] Reintentando PID %d (intento %d)\n", pid, task.Attempts)
					q.Receive <- task // reenviar para procesamiento
				}
			}
			q.mutex.Unlock()
		}
	}()

	log.Println("TTL Manager started.")
}

func (utm *Queue) AddTask(t providers.Process) {
	newTask := t

	newTask.Priority = 0
	newTask.PID = generateRandomPIDfromMap()

	switch newTask.DataSend.(type) {
	case comunication.Event:
		newTask.Type = "EVENT"
	case comunication.State:
		newTask.Type = "STATE"
	default:
		newTask.Type = "UNKNOWN"
	}

	// Bloquea solo para escribir en el mapa
	utm.mutex.Lock()
	if utm.Tasks == nil {
		utm.Tasks = make(map[int]*providers.Process)
	}
	utm.Tasks[newTask.PID] = &newTask
	utm.mutex.Unlock()

	// Ahora que ya liberaste el lock, puedes enviar al canal
	utm.Receive <- &newTask

}

func (utm *Queue) RemoveTaskByPID(pid int) {
	utm.mutex.Lock()
	defer utm.mutex.Unlock()
	delete(utm.Tasks, pid)
}

func (utm *Queue) GetTaskByPID(pid int) *providers.Process {
	utm.mutex.Lock()
	defer utm.mutex.Unlock()
	if task, exists := utm.Tasks[pid]; exists {
		return task
	}
	return nil
}

func (utm *Queue) GetTasks() map[int]*providers.Process {
	utm.mutex.Lock()
	defer utm.mutex.Unlock()
	return utm.Tasks
}

func (utm *Queue) Print() {
	utm.mutex.Lock()
	defer utm.mutex.Unlock()
	if len(utm.Tasks) == 0 {
		fmt.Println("No tasks in the queue.")
		return
	}
	fmt.Println("Current tasks in the queue:")
	for pid, task := range utm.Tasks {
		println("PID:", pid, "Task:", task.Type)
	}
}

func (utm *Queue) Start(clients *client.ClientSliceGroups, buffer int, workers int) {
	utm.Clients = clients
	utm.Receive = make(chan *providers.Process, buffer)
	for i := 0; i < workers; i++ {
		go func() {
			for proc := range utm.Receive {
				utm.mutex.Lock()
				// Process the task
				err := proc.ManageProccess(utm.Clients)
				if err != nil {
					log.Printf("Error processing PID %d: %v", proc.PID, err)
					proc.Priority += 1
				}
				utm.mutex.Unlock()
			}
		}()
	}

}

func (utm *Queue) Stop() {
	close(utm.Receive)
}

// StartTTLManager inicia el gestor de timeouts
/*func (uqm *UnifiedQueueManager) StartTTLManager(notificationManager NotificationManager) {
	uqm.ttlManager.notificationManager = notificationManager
	uqm.ttlManager.start()
}*/

// StopTTLManager detiene el gestor de timeouts
/*func (uqm *UnifiedQueueManager) StopTTLManager() {
	uqm.ttlManager.stop()
}*/

// StartQueuesWithTTL inicia todas las colas con TTL manager
/*func (uqm *UnifiedQueueManager) StartQueuesWithTTL(clients *client.ClientSliceGroups, buffer int, workers int, notificationManager NotificationManager) {
	uqm.StartQueues(clients, buffer, workers)
	uqm.StartTTLManager(notificationManager)
	log.Println("Unified Queue Manager started with TTL monitoring")
}*/

// StopQueues detiene todas las colas
