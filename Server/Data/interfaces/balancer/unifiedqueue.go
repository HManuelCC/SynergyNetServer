package balancer

import (
	"math/rand"
	"sync"

	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/client"
	"github.com/HManuelCC/SynergyNetServer/Server/Data/interfaces/providers"
)

/*Tasks   []*providers.ProcessEvent
Receive chan *providers.ProcessEvent
Clients *client.ClientSliceGroups
running bool*/

type Queue struct {
	Tasks                    map[int]*providers.Process //providers.Process
	Type                     string
	Clients                  *client.ClientSliceGroups
	EventsClientSubscripcion *client.ClientSliceGroupMapEventSubscription
	Mutex                    sync.Mutex
	Receive                  chan *providers.Process

	priorityBuckets map[int][]*providers.Process
	priorityRRIndex map[int]int
}

var BalancerQueue *Queue = &Queue{
	Tasks:           make(map[int]*providers.Process),
	Receive:         make(chan *providers.Process, 100),
	priorityBuckets: make(map[int][]*providers.Process),
	priorityRRIndex: make(map[int]int),
}

func generateRandomPIDfromMap() int {
	pid := rand.Intn(10000)
	if _, exists := BalancerQueue.Tasks[pid]; exists {
		return generateRandomPIDfromMap()
	}
	return pid

}
