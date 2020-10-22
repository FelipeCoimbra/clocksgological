package lclock

import (
	"errors"
	"sync"
	"time"
)

// ScalarTimestamp is the container for scalar timestamps.
type ScalarTimestamp uint32

// ScalarTimestampMax is the maximum possible timestamp to be committed.
const ScalarTimestampMax = ^ScalarTimestamp(0)

// ScalarEventCallback is a callback function to notify the new clock time after clock processing
type ScalarEventCallback func(ScalarTimestamp, error)

// ScalarEvent is the representation of a relevant scalar-stamped event to be commited by our scalar clock.
type ScalarEvent struct {
	timestamp ScalarTimestamp
	notify    ScalarEventCallback
}

// ScalarClock Logical Clock that maintains a scalar timestamp.
type ScalarClock struct {
	clockController
	eventChannel chan ScalarEvent
	timestamp    ScalarTimestamp
}

// Emit a new ScalarEvent to be stamped by the ScalarClock.
// It will block until the ScalarClock accepts the event proposal.
func (clock *ScalarClock) Emit(scalarEvent ScalarEvent) {
	clock.eventChannel <- scalarEvent
}

// Reset the clock timestamp back to zero.
func (clock *ScalarClock) Reset() {
	clock.timestamp = 0
}

// Time Returns the current timestamp.
func (clock *ScalarClock) Time() ScalarTimestamp {
	return clock.timestamp
}

// tick the ScalarClock to generate a new timestamp.
// The ScalarClock checks for queued events. If there's none, do nothing.
// If there's a new event, update the timestamp and notify if asked for.
func (clock *ScalarClock) tick() {
	select {
	case event, ok := <-clock.eventChannel:
		if ok {
			// Clock update
			var maxTimestamp ScalarTimestamp
			if clock.timestamp > event.timestamp {
				maxTimestamp = clock.timestamp
			} else {
				maxTimestamp = event.timestamp
			}
			if maxTimestamp < ScalarTimestampMax {
				clock.timestamp = maxTimestamp + 1 // Important: this must be a single assignment to keep atomicity
				// Breaking timestamp update in multiple instructions can cause the Time() method to return invalid timestamps.
				if event.notify != nil {
					event.notify(clock.timestamp, nil)
				}
			} else if event.notify != nil {
				event.notify(clock.timestamp, errors.New("Error: ScalarTimestamp would overflow"))
			}

		} else {
			// eventChannel was closed: no more events. Enqueue a stop signal.
			go clock.Stop()
		}
	default:
		// No message, nothing to do
	}
}

// NewScalarClock Creates a new valid ScalarClock.
// Initializes channels for control and triggering ticks
func NewScalarClock(tickChannelSize int) *ScalarClock {
	scalarClock := &ScalarClock{
		clockController: clockController{
			controlChannel: make(chan bool),
			controlMutex:   sync.Mutex{},
			loopSleep:      100 * time.Millisecond,
			state:          Standby,
		},
		timestamp: 0,
	}
	if tickChannelSize > 0 {
		scalarClock.eventChannel = make(chan ScalarEvent, tickChannelSize)
	} else {
		scalarClock.eventChannel = make(chan ScalarEvent)
	}
	scalarClock.clockController.tick = scalarClock.tick
	return scalarClock
}
