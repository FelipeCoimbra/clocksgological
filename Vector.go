package lclock

import (
	"errors"
	"sync"
	"time"
)

// VectorTimestamp is the container for vector timestamps.
type VectorTimestamp []uint32

// VectorTimestampMax is the maximum possible timestamp to be committed to a specific position of the VectorTimestamp.
const VectorTimestampMax = ^uint32(0)

// VectorEventCallback is a callback function to notify the new clock time after clock processing
type VectorEventCallback func(VectorTimestamp, error)

// VectorEvent is the representation of a relevant vector-stamped event to be commited by our vector clock.
type VectorEvent struct {
	timestamp VectorTimestamp
	notify    VectorEventCallback
}

// VectorClock Logical Clock that maintains a vector timestamp.
type VectorClock struct {
	clockController
	eventChannel chan VectorEvent
	eventMutex   sync.RWMutex
	timestamp    VectorTimestamp
	id           int
}

// Emit a new VectorEvent to be stamped by the VectorClock.
// It will block until the VectorClock accepts the event proposal.
func (clock *VectorClock) Emit(vectorEvent VectorEvent) {
	clock.eventChannel <- vectorEvent
}

// Reset the clock timestamp back to zero.
func (clock *VectorClock) Reset() {
	clock.eventMutex.Lock()
	defer clock.eventMutex.Unlock()
	clock.timestamp = make(VectorTimestamp, len(clock.timestamp))
}

// Time Returns the current timestamp.
func (clock *VectorClock) Time() VectorTimestamp {
	// RLocking and RUnlocking won't affect multiple readers!
	clock.eventMutex.RLock()
	defer clock.eventMutex.RUnlock()
	return clock.timestamp
}

// tick the VectorClock to generate a new timestamp.
// The VectorClock checks for queued events. If there's none, do nothing.
// If there's a new event, update the timestamp and notify if asked for.
func (clock *VectorClock) tick() {
	select {
	case event, ok := <-clock.eventChannel:
		if ok {
			if clock.timestamp[clock.id] == VectorTimestampMax {
				if event.notify != nil {
					event.notify(clock.timestamp, errors.New("Error: VectorTimestamp would overflow"))
				}
			} else if event.timestamp != nil && len(event.timestamp) != len(clock.timestamp) {
				// The timestamp lengths don't match, so I don't know how to deal with it
				if event.notify != nil {
					event.notify(clock.timestamp, errors.New("Error: VectorTimestamp length doesn't match the VectorClock's timestamp length"))
				}
			} else {
				clock.eventMutex.Lock()
				clock.timestamp[clock.id]++
				if event.timestamp != nil {
					for i, currentTime := range clock.timestamp {
						if event.timestamp[i] > currentTime {
							clock.timestamp[i] = event.timestamp[i]
						}
					}
				}
				clock.eventMutex.Unlock()
				if event.notify != nil {
					event.notify(clock.timestamp, nil)
				}
			}

		} else {
			// eventChannel was closed: no more events. Enqueue a stop signal.
			go clock.Stop()
		}
	default:
		// No message, nothing to do
	}
}

// NewVectorClock Creates a new valid VectorClock.
// Initializes channels for control and triggering ticks
func NewVectorClock(tickLoopSleepMSec int, tickChannelSize int, id int, processCount int) *VectorClock {
	if tickLoopSleepMSec < 0 {
		tickLoopSleepMSec = 0
	}
	if tickChannelSize < 0 {
		tickChannelSize = 0
	}
	if id <= 0 {
		// IDs are 1-indexed
		id = 1
	}
	if processCount < id {
		processCount = id // You should have at least <id> processes, given you have ID <id>
	}
	vectorClock := &VectorClock{
		clockController: clockController{
			controlChannel: make(chan bool),
			controlMutex:   sync.Mutex{},
			loopSleep:      time.Duration(tickLoopSleepMSec) * time.Millisecond,
			state:          Standby,
		},
		eventMutex: sync.RWMutex{},
		timestamp:  make(VectorTimestamp, processCount),
		id:         id - 1,
	}
	if tickChannelSize > 0 {
		vectorClock.eventChannel = make(chan VectorEvent, tickChannelSize)
	} else {
		vectorClock.eventChannel = make(chan VectorEvent)
	}
	vectorClock.clockController.tick = vectorClock.tick
	return vectorClock
}

// NewDefaultVectorClock Creates a new valid VectorClock with Default values.
// Default parameters: loopSleep = 100msec, tickChannelSize = 1024 (~4KB of memory).
func NewDefaultVectorClock(id int, processCount int) *VectorClock {
	return NewVectorClock(100, 1024, id, processCount)
}
