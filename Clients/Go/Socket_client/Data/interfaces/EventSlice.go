package interfaces

type EventString struct {
	Name          string                                                              `json:"name"`
	EventProccess func(event Event, conn *Client, messagePid int, destination string) `json:"eventProccess"`
}

type EventsSubscribed struct {
	Events []string `json:"events"`
}

type EventSlice []EventString

var EventSliceInstance *EventsSubscribed = &EventsSubscribed{Events: []string{}}

func (e *EventSlice) AddEvent(event string, handleFunction func(event Event, conn *Client, messagePid int, destination string)) {
	*e = append(*e, EventString{Name: event, EventProccess: handleFunction})
	EventSliceInstance.Events = append(EventSliceInstance.Events, event)
}

func (e *EventSlice) RemoveEvent(event string) {
	for i, v := range *e {
		if v.Name == event {
			*e = append((*e)[:i], (*e)[i+1:]...)
		}
	}
	for i, v := range EventSliceInstance.Events {
		if v == event {
			EventSliceInstance.Events = append(EventSliceInstance.Events[:i], EventSliceInstance.Events[i+1:]...)
		}
	}
}

func (e *EventSlice) Len() int64 {
	return int64(len(*e))
}
