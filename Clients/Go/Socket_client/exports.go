package SynergyNetClient

import (
	"github.com/HManuelCC/SynergyNetClient/Socket_client/Data/interfaces"
)

// Re-export commonly used types from the internal interfaces package so
// consumers can use SynergyNetClient.Event, SynergyNetClient.State, etc.
// These are type aliases (no copies) and do not change functionality.

type (
	Event            = interfaces.Event
	State            = interfaces.State
	MessageState     = interfaces.MessageState
	Client           = interfaces.Client
	EventSliceType   = interfaces.EventSlice
	EventsSubscribed = interfaces.EventsSubscribed
	Process          = interfaces.Process
	ResponseCallback = interfaces.ResponseCallback
)

// Also re-export the global EventSlice variable (already declared in package
// SynergyNetClient in Client.go). Users can refer to SynergyNetClient.EventSlice
// directly. No additional variables are required here; this file only provides
// type-level aliases for convenience.
