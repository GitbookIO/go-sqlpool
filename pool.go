package sqlpool

import (
	"database/sql"
	"strings"
	"sync"

	"github.com/GitbookIO/go-sqlpool/utils/condgroup"
	"github.com/GitbookIO/go-sqlpool/utils/counter"
)

type Opts struct {
	Max int

	// Init functions
	PreInit  func(driver, url string) error
	PostInit func(db *sql.DB) error
}

type Pool struct {
	opts Opts
	rw   sync.RWMutex

	databases map[string]*Resource
	inactive  map[string]*Resource
	conds     *condgroup.CondGroup
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
		conds:     condgroup.NewCondGroup(),
	}
}

// What our Pool tracks
type Resource struct {
	DB     *sql.DB
	Driver string
	Url    string

	// Private fields used to track resource usage
	users      counter.Counter
	lastActive int
}

func (r *Resource) Key() string {
	return key(r.Driver, r.Url)
}

func (p *Pool) Acquire(driver, url string) (*Resource, error) {
	// Actually get resource
	resource, err := p.open(driver, url)
	if err != nil {
		return nil, err
	}

	// Update resource's usage
	p.acquire(resource)

	return resource, nil
}

func (p *Pool) Release(r *Resource) error {
	// Update usage
	p.release(r)

	// Mark as idle
	if r.users.Value() == 0 {

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

func (p *Pool) acquire(r *Resource) {
	r.users.Increment()
}

func (p *Pool) release(r *Resource) {
	r.users.Decrement()
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
		p.databases[key(driver, url)] = &Resource{
			DB:     db,
			Driver: driver,
			Url:    url,
		}
	}

	return p.get(driver, url), nil
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
