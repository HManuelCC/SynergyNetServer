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

	// 1. Obtener la tarea
	utm.mutex.Lock()                          // ¡Bloquear ANTES de get!
	task := utm.GetTaskByPID(state.ServerPID) // Necesitarás crear esta función
	if task == nil {
		utm.mutex.Unlock()
		return ErrProcessNotFound
	}

	// 2. Modificar la tarea DENTRO del lock
	if state.Status {
		if state.ProcessStatus == 2 {
			// Eliminará la tarea (esto necesita su propio lock, así que debemos crear una versión _noLock)
			utm.RemoveTaskByPID(state.ServerPID)
		} else {
			task.Updated = time.Now()
		}
	} else {
		task.Attempts--
		if task.Attempts <= 0 {
			utm.RemoveTaskByPID(state.ServerPID)
			utm.mutex.Unlock() // Desbloquear antes de retornar error
			return fmt.Errorf("%w%s", ErrProcessFailed, state.Error)
		}
	}

	// 3. Desbloquear al final
	utm.mutex.Unlock()
	return nil
}

func (q *Queue) StartTTLManager() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			q.mutex.Lock()
			var tasksToRetry []*providers.Process
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
					fmt.Printf("[TTL] PID %d ha excedido TTL, reintentando. Intentos restantes: %d\n", pid, task.Attempts)
					tasksToRetry = append(tasksToRetry, task)
				}
			}
			q.mutex.Unlock()

			for _, task := range tasksToRetry {
				q.Receive <- task
			}
		}
	}()

	log.Println("TTL Manager started.")
}

func (utm *Queue) AddTask(t providers.Process) {
	fmt.Println("Adding task to Unified Queue Manager:", t.Type)
	newTask := t

	newTask.Priority = 0
	newTask.PID = generateRandomPIDfromMap()

	// Bloquea solo para escribir en el mapa
	utm.mutex.Lock()
	if utm.Tasks == nil {
		utm.Tasks = make(map[int]*providers.Process)
	}
	utm.Tasks[newTask.PID] = &newTask
	/*pr := t.Priority
	utm.priorityBuckets[pr] = append(utm.priorityBuckets[pr], &newTask)*/
	utm.mutex.Unlock()

	// Ahora que ya liberaste el lock, puedes enviar al canal
	utm.Receive <- &newTask

}

func (utm *Queue) nextTaskByPriority() *providers.Process {
	if len(utm.priorityBuckets) == 0 {
		return nil
	}

	maxPrio := -1

	for pr := range utm.priorityBuckets {
		if pr > maxPrio {
			maxPrio = pr
		}
	}

	bucket := utm.priorityBuckets[maxPrio]
	if len(bucket) == 0 {
		delete(utm.priorityBuckets, maxPrio)
		return utm.nextTaskByPriority()
	}
	idx := utm.priorityRRIndex[maxPrio] % len(bucket)
	proc := bucket[idx]

	utm.priorityRRIndex[maxPrio] = (utm.priorityRRIndex[maxPrio] + 1) % len(bucket)

	return proc
}

func (utm *Queue) RemoveTaskByPID(pid int) {

	delete(utm.Tasks, pid)
}

func (utm *Queue) GetTaskByPID(pid int) *providers.Process {

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

func (utm *Queue) Start(clients *client.ClientSliceGroups, buffer int, workers int, eventsClientSubscripcion *client.ClientSliceGroupMapEventSubscription) {
	utm.Clients = clients
	utm.EventsClientSubscripcion = eventsClientSubscripcion
	for i := 0; i < workers; i++ {
		go func() {
			for proc := range utm.Receive {

				// Process the task
				err := proc.ManageProccess(utm.Clients, utm.EventsClientSubscripcion)
				if err != nil {
					log.Printf("Error processing PID %d: %v", proc.PID, err)
					utm.mutex.Lock()
					proc.Priority += 1
					utm.mutex.Unlock()
				}

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
