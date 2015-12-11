package condgroup

import (
	"fmt"
	"sync"
)

type CondGroup struct {
	groups map[string]*sync.Cond
	lock   *sync.Mutex
}

func NewCondGroup() *CondGroup {
	return &CondGroup{
		groups: map[string]*sync.Cond{},
		lock:   &sync.Mutex{},
	}
}

// Lock returns true if the caller is the first to lock for this key
func (cg *CondGroup) Lock(key string) (first bool) {
	// Lock to prevent race conditions
	cg.lock.Lock()

	// Check if we have an existing group
	if cond, ok := cg.groups[key]; ok {
		// Unlock so other callers can come in
		cg.lock.Unlock()
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
		return false
	}
	// Unlock when we're finished setting up the new cond
	defer cg.lock.Unlock()

	// So we're the first caller
	cond := sync.NewCond(&sync.Mutex{})

	// Add cond for group
	cg.groups[key] = cond

	return true
}

// Unlock should only be called by the original caller of "Lock"
// (that got the "true" return value)
// All subsequent callers to "Lock" are now unpaused
func (cg *CondGroup) Unlock(key string) error {
	cg.lock.Lock()
	defer cg.lock.Unlock()

	cond, ok := cg.groups[key]
	if !ok {
		return fmt.Errorf("Can not unlock group %s: no lock exists", key)
	}

	// Free all wailting funcs
	cond.Broadcast()

	// Delete cond
	delete(cg.groups, key)

	return nil
}
