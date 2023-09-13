package internal

import "sync"

type MutexByFrame struct {
	mu       *sync.Mutex // protects the map
	frameToMu map[string]*sync.Mutex
}

func NewMutexByFrame() *MutexByFrame {
	return &MutexByFrame{
		mu:       &sync.Mutex{},
		frameToMu: make(map[string]*sync.Mutex),
	}
}

func (m *MutexByFrame) Lock(frameID string) {
	m.mu.Lock()
	frameMu := m.frameToMu[frameID]
	if frameMu == nil {
		frameMu = &sync.Mutex{}
	}
	m.frameToMu[frameID] = frameMu
	m.mu.Unlock()
	// don't lock inside m.mu else we can deadlock
	frameMu.Lock()
}

func (m *MutexByFrame) Unlock(frameID string) {
	m.mu.Lock()
	frameMu := m.frameToMu[frameID]
	if frameMu == nil {
		panic("MutexByFrame: Unlock before Lock")
	}
	m.mu.Unlock()

	frameMu.Unlock()
}
