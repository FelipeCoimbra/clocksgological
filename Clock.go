package lclock

import (
	"sync"
	"time"
)

// ClockState is the possible state of a clock.
type ClockState int

const (
	standby ClockState = iota
	running            = iota
	paused             = iota
	stopped            = iota
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
// This method has no effect if the clockController is not on running state.
// It will block until the clock worker accepts the pause command.
func (controller *clockController) Pause() {
	controller.controlMutex.Lock()
	defer controller.controlMutex.Unlock()
	if controller.state == running {
		controller.controlChannel <- true
		controller.state = paused
	}
}

// Resume the clock.
// This method has no effect if the clock is not on paused state.
// It will block until the clock worker accepts the resume command.
func (controller *clockController) Resume() {
	controller.controlMutex.Lock()
	defer controller.controlMutex.Unlock()
	if controller.state == paused {
		controller.controlChannel <- false
		controller.state = running
	}
}

// Run the ScalarClock.
// This method has no effect if the clock is already running.
// This call runs indefinitely. Call concurrently as to keep thread control.
func (controller *clockController) Run() {
	controller.controlMutex.Lock()
	if controller.state != standby {
		return
	}
	controller.state = running
	controller.controlMutex.Unlock()
	alive := true
	for alive {
		select {
		case controlSignal, ok := <-controller.controlChannel:
			if !ok {
				// Clock was stopped during running state.
				alive = false
				break
			}
			// Control signal issued. Hold until liberation
			for controlSignal && ok {
				controlSignal, ok = <-controller.controlChannel
			}
			if !ok {
				// Clock was stopped during paused state.
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
// This method has no effect if the clock is not on running state.
// After this call, the clock worker will no longer consume Events.
func (controller *clockController) Stop() {
	controller.controlMutex.Lock()
	defer controller.controlMutex.Unlock()
	if controller.state == running || controller.state == paused {
		close(controller.controlChannel)
		controller.state = stopped
	}
}
