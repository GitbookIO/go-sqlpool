package sqlpool

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GitbookIO/syncgroup"
)

type Opts struct {
	Max         int64
	IdleTimeout int64

	// Init functions
	PreInit  func(driver, url string) error
	PostInit func(db *sql.DB) error
}

type Pool struct {
	opts Opts
	rw   sync.RWMutex

	databases map[string]*Resource
	inactive  map[string]*Resource
	conds     *syncgroup.CondGroup
}

type Stats struct {
	Total    int
	Active   int
	Inactive int
}

func NewPool(opts Opts) *Pool {
	return &Pool{
		opts:      opts,
		rw:        sync.RWMutex{},
		databases: map[string]*Resource{},
		inactive:  map[string]*Resource{},
		conds:     syncgroup.NewCondGroup(),
	}
}

// What our Pool tracks
type Resource struct {
	DB     *sql.DB
	Driver string
	Url    string

	// Private fields used to track resource usage
	users      syncgroup.ActiveCounter
	lastActive int64
}

func (r *Resource) Key() string {
	return key(r.Driver, r.Url)
}

func (p *Pool) Acquire(driver, url string) (*Resource, error) {
	// Actually get resource
	resource, err := p.open(driver, url)
	if err != nil {
		return nil, err
	} else if resource == nil {
		return nil, fmt.Errorf("Failed to open %s://%s for an unknown reason", driver, url)
	}

	// Update resource's usage
	p.acquire(resource)

	return resource, nil
}

func (p *Pool) Release(r *Resource) error {
	// Update resource's usage
	p.release(r)

	// Mark as idle
	if !r.users.IsActive() {
		p.rw.Lock()
		p.inactive[r.Key()] = r
		p.rw.Unlock()

		// Do cleanup
		// TODO: lazily
		return p.Cleanup()
	}

	return nil
}

func (p *Pool) Close() error {
	return p.close(false)
}

func (p *Pool) ForceClose() error {
	return p.close(true)
}

func (p *Pool) close(force bool) error {
	p.rw.Lock()
	defer p.rw.Unlock()

	for key, resource := range p.databases {
		// Exit if we're not force closing
		if err := resource.DB.Close(); err != nil && !force {
			return err
		}
		p.removeResource(key)
	}

	return nil
}

// Cleanup removes old/inactive connections
func (p *Pool) Cleanup() error {
	// Write lock
	p.rw.Lock()
	defer p.rw.Unlock()

	// Current timestamp
	now := time.Now().Unix()

	for key, resource := range p.inactive {
		// Skip if still valid
		if (now - p.opts.IdleTimeout) < resource.lastActive {
			continue
		}

		// Remove from inactive list and databases
		delete(p.databases, key)
		delete(p.inactive, key)

		// Close database
		go func(r *Resource) {
			p.cleanupResource(r)
		}(resource)
	}

	return nil
}

func (p *Pool) Stats() Stats {
	total := len(p.databases)
	inactive := len(p.inactive)
	active := total - inactive

	return Stats{
		Total:    total,
		Active:   active,
		Inactive: inactive,
	}
}

func (p *Pool) cleanupResource(r *Resource) {
	// Close database
	if err := r.DB.Close(); err != nil {
		// TODO: log failure
	}
}

func (p *Pool) acquire(r *Resource) {
	r.users.Inc()
	r.lastActive = time.Now().Unix()
}

func (p *Pool) release(r *Resource) {
	r.users.Dec()
	r.lastActive = time.Now().Unix()
}

func (p *Pool) open(driver, url string) (*Resource, error) {
	// DB already opened
	if p.has(driver, url) {
		return p.get(driver, url), nil
	}

	// Open DB: only one should do this, everyone else should wait
	if p.conds.Lock(key("open", driver, url)) {
		defer p.conds.Unlock(key("open", driver, url))
		// Before opening DB
		if p.opts.PreInit != nil {
			if err := p.opts.PreInit(driver, url); err != nil {
				return nil, err
			}
		}

		// Open DB
		db, err := sql.Open(driver, url)
		if err != nil {
			return nil, err
		}

		// After opening DB
		if p.opts.PostInit != nil {
			if err := p.opts.PostInit(db); err != nil {
				return nil, err
			}
		}

		// Add db resource
		p.rw.Lock()
		p.databases[key(driver, url)] = &Resource{
			DB:     db,
			Driver: driver,
			Url:    url,
		}
		p.rw.Unlock()
	}

	return p.get(driver, url), nil
}

func (p *Pool) removeResource(key string) {
	delete(p.databases, key)
	delete(p.inactive, key)
}

func (p *Pool) get(driver, url string) *Resource {
	p.rw.RLock()
	defer p.rw.RUnlock()
	return p.databases[key(driver, url)]
}

func (p *Pool) has(driver, url string) bool {
	return p.get(driver, url) != nil
}

func key(strs ...string) string {
	return strings.Join(strs, ":")
}
