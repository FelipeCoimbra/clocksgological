package lclock

import (
	"sync"
	"time"
)

// ClockState is the possible state of a clock.
type ClockState int

const (
	// Standby is the initial state of a clock. The clock does not accept ticks and its
	// timestamp is at reset value. Will change to Running at a Run() call.
	Standby ClockState = iota
	// Running is the normal state of a running clock, accepting ticks normally.
	// May change to Paused by a Pause() call or Stopped by a Stop() call.
	Running = iota
	// Paused is a recoverable non-running state. The clock will not accept ticks at this
	// state but may change to Running through a Resume() call. It may also close definitely
	// by a Stop() call.
	Paused = iota
	// Stopped is a state of definite idleness. The clock will not accept ticks and its
	// current time value will only change if directly intervened.
	Stopped = iota
)

// ClockController is the interface for controlling the execution state of a LogicalClock
type ClockController interface {
	Pause()
	Resume()
	Run()
	State() ClockState
	Stop()
}

// clockController controls the activity of a clock
type clockController struct {
	ClockController
	controlChannel chan bool  // The channel where to issue state changes
	controlMutex   sync.Mutex // To synchronize state changes from concurrent consumers of clockController
	loopSleep      time.Duration
	state          ClockState // The clock execution state
	tick           func()     // The triggering function of the clock
}

func (controller *clockController) SetUpdatePeriod(newLoopSleep time.Duration) {
	controller.loopSleep = newLoopSleep
}

// Pause the clock.
// This method has no effect if the clockController is not on Running state.
// It will block until the clock worker accepts the pause command.
func (controller *clockController) Pause() {
	controller.controlMutex.Lock()
	defer controller.controlMutex.Unlock()
	if controller.state == Running {
		controller.controlChannel <- true
		controller.state = Paused
	}
}

// Resume the clock.
// This method has no effect if the clock is not on Paused state.
// It will block until the clock worker accepts the resume command.
func (controller *clockController) Resume() {
	controller.controlMutex.Lock()
	defer controller.controlMutex.Unlock()
	if controller.state == Paused {
		controller.controlChannel <- false
		controller.state = Running
	}
}

// Run the ScalarClock.
// This method has no effect if the clock is already running.
// This call runs indefinitely. Call concurrently as to keep thread control.
func (controller *clockController) Run() {
	controller.controlMutex.Lock()
	if controller.state != Standby {
		return
	}
	controller.state = Running
	controller.controlMutex.Unlock()
	alive := true
	for alive {
		select {
		case controlSignal, ok := <-controller.controlChannel:
			if !ok {
				// Clock was stopped during Running state.
				alive = false
				break
			}
			// Control signal issued. Hold until liberation
			for controlSignal && ok {
				controlSignal, ok = <-controller.controlChannel
			}
			if !ok {
				// Clock was stopped during Paused state.
				alive = false
				break
			}
		default:
			if controller.tick != nil {
				controller.tick()
			}
			time.Sleep(controller.loopSleep) // ...as not to push 100% CPU usage
		}
	}
}

// State returns the current execution state of the clock.
func (controller *clockController) State() ClockState {
	return controller.state
}

// Stop the clock.
// This method has no effect if the clock is not on Running state.
// After this call, the clock worker will no longer consume Events.
func (controller *clockController) Stop() {
	controller.controlMutex.Lock()
	defer controller.controlMutex.Unlock()
	if controller.state == Running || controller.state == Paused {
		close(controller.controlChannel)
		controller.state = Stopped
	}
}
