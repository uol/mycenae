package embassy

import (
	"errors"
	"time"

	"github.com/hashicorp/consul/api"

	"go.uber.org/zap"
)

type ServiceWatcher interface {
	Health(si *api.ServiceEntry, err error)
	Fail(si *api.ServiceEntry, err error)
}

type ServiceSettings struct {
	Name         string
	ID           string
	Tags         []string
	Address      string
	Port         int
	PoolInterval string
}

func NewService(log *zap.Logger, settings ServiceSettings) (*Service, error) {
	if consulClient == nil {
		return nil, errors.New("No agent connection found")
	}

	dur, err := time.ParseDuration(settings.PoolInterval)

	if err != nil {
		return nil, err
	}

	if settings.ID == "" {
		settings.ID = settings.Name
	}

	srvc := &Service{
		log:      log,
		settings: settings,
		client:   consulClient,
		poolInt:  dur,
		skac:     make(chan struct{}),
		swsc:     make(chan struct{}),
		sis:      make(map[string]*api.ServiceEntry),
	}
	return srvc, nil
}

type Service struct {
	log      *zap.Logger
	settings ServiceSettings
	client   *api.Client
	kaOn     bool
	watching bool
	ttlName  string
	poolInt  time.Duration
	sw       ServiceWatcher
	sis      map[string]*api.ServiceEntry
	skac     chan struct{}
	swsc     chan struct{}
}

func (srvc *Service) Register() error {
	agent := srvc.client.Agent()
	asr := &api.AgentServiceRegistration{
		ID:      srvc.settings.ID,
		Name:    srvc.settings.Name,
		Tags:    srvc.settings.Tags,
		Address: srvc.settings.Address,
		Port:    srvc.settings.Port,
	}

	return agent.ServiceRegister(asr)
}

func (srvc *Service) Remove() error {
	agent := srvc.client.Agent()
	return agent.ServiceDeregister(srvc.settings.ID)
}

func (srvc *Service) AddHTTPcheck(name, path, interval string) error {
	agent := srvc.client.Agent()

	check := &api.AgentCheckRegistration{
		ID:        name,
		Name:      name,
		ServiceID: srvc.settings.ID,
	}

	check.HTTP = path
	check.Interval = interval

	return agent.CheckRegister(check)
}

func (srvc *Service) AddTTLcheck(name, interval string) error {
	if srvc.ttlName != "" && srvc.ttlName != name {
		return errors.New("Only one TTL check in suported")
	}

	agent := srvc.client.Agent()
	check := &api.AgentCheckRegistration{
		ID:        name,
		Name:      name,
		ServiceID: srvc.settings.ID,
	}

	check.TTL = interval
	if err := agent.CheckRegister(check); err != nil {
		return err
	}

	srvc.ttlName = name
	return nil
}

func (srvc *Service) RemoveCheck(checkID string) error {
	agent := srvc.client.Agent()
	return agent.CheckDeregister(checkID)
}

func (srvc *Service) AddJobCheck(name, interval string) error {
	agent := srvc.client.Agent()

	check := &api.AgentCheckRegistration{
		ID:        name,
		Name:      name,
		ServiceID: srvc.settings.ID,
	}

	check.Interval = interval
	return agent.CheckRegister(check)
}

func (srvc *Service) PassTTL() {

	agent := srvc.client.Agent()
	if err := agent.PassTTL(srvc.ttlName, ""); err != nil {
		srvc.log.Error(
			err.Error(),
			zap.String("struct", "Service"),
			zap.String("func", "PassTTL"),
			zap.Error(err),
		)
	}
}

func (srvc *Service) WarnTTL() {

	agent := srvc.client.Agent()
	if err := agent.WarnTTL(srvc.ttlName, ""); err != nil {
		srvc.log.Error(
			err.Error(),
			zap.String("struct", "Service"),
			zap.String("func", "WarnTTL"),
			zap.Error(err),
		)
	}
}

func (srvc *Service) FailTTL() {

	agent := srvc.client.Agent()
	if err := agent.FailTTL(srvc.ttlName, ""); err != nil {
		srvc.log.Error(
			err.Error(),
			zap.String("struct", "Service"),
			zap.String("func", "FailTTL"),
			zap.Error(err),
		)
	}
}

// StartKeepAlive uses AddTTLcheck and automatically
// sends an ping to consul using an goroutine.
func (srvc *Service) StartKeepAlive(interval string) error {
	srvc.skac = make(chan struct{})
	if err := srvc.AddTTLcheck("autoKeepAlive", interval); err != nil {
		return err
	}

	dur, err := time.ParseDuration(interval)
	if err != nil {
		return err
	}

	go func() {
		srvc.kaOn = true
		ticker := time.NewTicker(dur).C

		for {
			select {
			case <-ticker:
				srvc.PassTTL()
			case <-srvc.skac:
				srvc.log.Debug("Terminating keep alive")
				return
			}
		}
	}()
	return nil
}

func (srvc *Service) StopKeepAlvie() {
	if srvc.kaOn {
		srvc.kaOn = false
		srvc.skac <- struct{}{}
	}
}

func (srvc *Service) WatchService(w ServiceWatcher) {
	srvc.sw = w

	go func() {
		ticker := time.NewTicker(srvc.poolInt).C
		srvc.watching = true

		for {
			select {
			case <-ticker:
				goodOnes, err := srvc.GetHealthyOnes()
				if err != nil {
					go srvc.sw.Health(nil, err)
					break
				}

				for _, good := range goodOnes {
					if si, ok := srvc.sis[good.Node.Node]; ok {
						if si.Service.Port != good.Service.Port || si.Service.Address != good.Service.Address {
							srvc.sis[good.Node.Node] = good
							go srvc.sw.Health(srvc.sis[good.Node.Node], nil)
						}
					} else {
						srvc.sis[good.Node.Node] = good
						go srvc.sw.Health(srvc.sis[good.Node.Node], nil)
					}
				}
				for k := range srvc.sis {
					found := false
					for _, good := range goodOnes {
						if k == good.Node.Node {
							found = true
						}
					}
					if !found {
						go srvc.sw.Fail(srvc.sis[k], nil)
						delete(srvc.sis, k)
					}
				}
			case <-srvc.swsc:
				return
			}
		}
	}()
}

func (srvc *Service) StopWathing() {
	if srvc.watching {
		srvc.watching = false
		srvc.swsc <- struct{}{}
	}
}

func (srvc *Service) GetHealthyOnes() ([]*api.ServiceEntry, error) {
	sh, _, err := srvc.client.Health().Service(srvc.settings.Name, "", true, nil)
	return sh, err
}
