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
	Tasks   map[int]*providers.Process //providers.Process
	Type    string
	Clients *client.ClientSliceGroups
	mutex   sync.Mutex
	Receive chan *providers.Process
}

var BalancerQueue *Queue = &Queue{Tasks: make(map[int]*providers.Process)}

func generateRandomPIDfromMap() int {
	pid := rand.Intn(10000)
	if _, exists := BalancerQueue.Tasks[pid]; exists {
		return generateRandomPIDfromMap()
	}
	return pid

}
