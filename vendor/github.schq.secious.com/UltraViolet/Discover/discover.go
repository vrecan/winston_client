package discover

import (
	// log "github.com/cihub/seelog"
	"fmt"
	log "github.com/cihub/seelog"
	consul "github.com/hashicorp/consul/api"
	"sort"
	"strconv"
	"sync"
	"time"
)

//Discover is the api to find services.
type Discover struct {
	cli           *consul.Client
	servicesCache *servicesCache
}

type servicesCache struct {
	services map[string]URLS
	*sync.Mutex
}

type URLS struct {
	URLS  []string
	timer *time.Timer
}

//Service is the description of a service.
type Service struct {
	Name string
	Addr string
	Port int
	Tags []string
}

//ServiceRequest allows you to set the options for your request.
type ServiceRequest struct {
	Name           string
	Tag            string
	IncludeFailing bool
	AllowStale     bool
	Near           string //Near requires a string to sort near a service _agent is used for sorting near yourself
	DataCenter     string
	Inconsistent   bool //this will make the read inconsistent but faster
	Token          string
	WaitIndex      uint64
	WaitTime       time.Duration
}

const cacheTimer = 5 * time.Second

var ERR_SERVICE_REBALANCING = fmt.Errorf("service is rebalancing")

//NewDiscover builds a new Discover object which allows you to find other services.
func NewDiscover() (*Discover, error) {
	client, err := consul.NewClient(consul.DefaultConfig())
	if nil != err {
		return nil, err
	}
	return &Discover{cli: client,
		servicesCache: &servicesCache{
			services: make(map[string]URLS, 0),
			Mutex:    &sync.Mutex{},
		},
	}, err
}

//Services Returns only healthy services
func (d Discover) Services(s ServiceRequest) ([]*consul.ServiceEntry, error) {
	var passingOnly bool
	var requireConsistent bool
	passingOnly = !s.IncludeFailing     //default is passingOnly
	requireConsistent = !s.Inconsistent //default is consistent

	opts := consul.QueryOptions{
		AllowStale:        s.AllowStale,
		Near:              s.Near,
		Datacenter:        s.DataCenter,
		RequireConsistent: requireConsistent,
		Token:             s.Token,
		WaitIndex:         s.WaitIndex,
		WaitTime:          s.WaitTime,
	}

	entry, _, err := d.cli.Health().Service(s.Name, s.Tag, passingOnly, &opts)
	return entry, err
}

//ServicesURLS will give you urls for all the services
func (d Discover) ServiceURLS(req ServiceRequest) (urls []string, err error) {
	urls = make([]string, 0)
	srv, err := d.Services(req)
	if err != nil {
		return urls, err
	}
	for _, s := range srv {
		url := fmt.Sprintf("%v:%s", s.Node.Address, strconv.Itoa(s.Service.Port))
		urls = append(urls, url)
	}
	sort.Strings(urls)
	return urls, err
}

//ServiceURLSCache will cache multiple requests and only query consul when it's exceeded
//the cache timeout
func (d Discover) ServiceURLSCache(serviceName string) (urls []string, err error) {
	d.servicesCache.Lock()
	defer d.servicesCache.Unlock()
	serv, ok := d.servicesCache.services[serviceName]
	if !ok {
		urls, err = d.ServiceURLS(ServiceRequest{Name: serviceName})
		if err != nil {
			return urls, err
		}
		d.servicesCache.services[serviceName] = URLS{URLS: urls, timer: time.NewTimer(cacheTimer)}
		return urls, err
	}

	select {
	case <-serv.timer.C:
		serv.timer.Reset(cacheTimer)
		log.Debug("discovering all services for ", serviceName)
		urls, err = d.ServiceURLS(ServiceRequest{Name: serviceName})
		if err != nil {
			log.Error("ServiceURLSCache - Failed to discover service: ", serviceName, " err: ", err)
			//return previous copy without error
			return serv.URLS, nil
		}
		log.Debug(serviceName, ": ", urls)
	default:
		urls = serv.URLS
		break
	}
	if len(urls) > len(serv.URLS) || len(urls) < len(serv.URLS) {
		//only error on rebalancing if we have previous urls
		if len(serv.URLS) != 0 {
			err = ERR_SERVICE_REBALANCING
		}
	}
	serv.URLS = urls
	d.servicesCache.services[serviceName] = serv
	return serv.URLS, err
}

//Close discover.
func (d *Discover) Close() error {
	if d != nil {
		d.servicesCache.Lock()
		defer d.servicesCache.Unlock()
		for _, s := range d.servicesCache.services {
			s.timer.Stop()
		}

	}
	return nil
}
