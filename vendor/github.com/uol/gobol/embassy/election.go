package embassy

import (
	"errors"
	"time"

	"github.com/hashicorp/consul/api"

	"go.uber.org/zap"
)

type Locker interface {
	GotElected(err error)
	LostMaster(err error)
}

type MasterWatcher interface {
	MasterInfo(name string, found bool, err error)
}

type ElectionSettings struct {
	Key           string
	PoolInterval  string
	ElectionRetry string
}

type masterInfo struct {
	name  string
	found bool
}

func NewElection(log *zap.Logger, settings ElectionSettings) (*Election, error) {

	if consulClient == nil {
		return nil, errors.New("No agent connection found")
	}

	dur, err := time.ParseDuration(settings.PoolInterval)

	if err != nil {
		return nil, err
	}

	retry, err := time.ParseDuration(settings.ElectionRetry)

	if err != nil {
		return nil, err
	}

	election := &Election{
		log:             log,
		settings:        settings,
		client:          consulClient,
		poolInt:         dur,
		retry:           retry,
		agent:           consulClient.Agent(),
		stopLock:        make(chan struct{}),
		stopLockWatch:   make(chan struct{}),
		stopCheckMaster: make(chan struct{}),
	}

	return election, nil

}

type Election struct {
	log             *zap.Logger
	settings        ElectionSettings
	client          *api.Client
	agent           *api.Agent
	lock            *api.Lock
	elected         bool
	retry           time.Duration
	lAct            Locker
	wmAct           MasterWatcher
	mi              masterInfo
	watching        bool
	locking         bool
	poolInt         time.Duration
	stopLock        chan struct{}
	stopCheckMaster chan struct{}
	stopLockWatch   chan struct{}
	lockChan        <-chan struct{}
}

func (l *Election) NewCandidate(actions Locker) {

	l.lAct = actions

	go l.asyncLock()

}

func (l *Election) StepDown() error {

	var err error

	if l.elected {
		l.stopLockWatch <- struct{}{}
		err = l.lock.Unlock()
	}

	if l.watching {
		l.stopCheckMaster <- struct{}{}
	}

	if l.locking {
		l.stopLock <- struct{}{}
	}

	return err

}

func (l *Election) WatchMaster(action MasterWatcher) {

	l.wmAct = action

	go l.asyncCheckMaster()

}

func (l *Election) StopWathing() {

	if l.watching {

		l.watching = false

		l.stopCheckMaster <- struct{}{}

	}

}

func (l *Election) asyncLock() {

	lc, err := l.getLock()

	if err != nil {

		l.log.Error(
			err.Error(),
			zap.String("struct", "Election"),
			zap.String("func", "asyncLock"),
			zap.Error(err),
		)

		time.Sleep(l.retry)

		go l.asyncLock()

		go l.lAct.GotElected(err)

		return
	}

	if lc == nil {
		return
	}

	l.lockChan = lc
	l.elected = true

	go l.lAct.GotElected(nil)

	go l.asyncLockWatch()

}

func (l *Election) asyncLockWatch() {

	for {
		select {
		case <-l.lockChan:

			go l.asyncLock()

			l.elected = false

			go l.lAct.LostMaster(nil)
			err := l.lock.Unlock()
			if err != nil {
				l.log.Error(
					err.Error(),
					zap.String("struct", "Election"),
					zap.String("func", "asyncLockWatch"),
					zap.Error(err),
				)
			}

			return

		case <-l.stopLockWatch:
			l.log.Info(
				"leave received",
				zap.String("struct", "Election"),
				zap.String("func", "asyncLockWatch"),
			)
			return
		}
	}

}

func (l *Election) getLock() (<-chan struct{}, error) {

	nn, err := l.agent.NodeName()

	if err != nil {
		return nil, err
	}

	l.log.Info(
		"got node name",
		zap.String("node name", nn),
		zap.String("struct", "Election"),
		zap.String("func", "getLock"),
	)

	lock, err := l.client.LockOpts(&api.LockOptions{
		Key:   l.settings.Key,
		Value: []byte(nn),
	})

	if err != nil {
		return nil, err
	}

	l.lock = lock

	l.log.Debug(
		"lock struct created",
		zap.String("node name", nn),
		zap.String("struct", "Election"),
		zap.String("func", "getLock"),
	)

	l.log.Info(
		"trying to aquire lock...",
		zap.String("node name", nn),
		zap.String("struct", "Election"),
		zap.String("func", "getLock"),
	)

	l.locking = true

	ll, err := l.lock.Lock(l.stopLock)

	l.locking = false

	if err != nil {
		return nil, err
	}

	if ll == nil {
		l.log.Debug(
			"no lock",
			zap.String("node name", nn),
			zap.String("struct", "Election"),
			zap.String("func", "getLock"),
		)
		return nil, nil
	}

	l.log.Info(
		"got lock",
		zap.String("node name", nn),
		zap.String("struct", "Election"),
		zap.String("func", "getLock"),
	)

	return ll, err

}

func (l *Election) asyncCheckMaster() {

	l.watching = true

	time.Sleep(2 * time.Second)

	ticker := time.NewTicker(l.poolInt).C

	for {
		select {
		case <-ticker:
			name, found, err := l.getMasterInfo()

			if err != nil {
				go l.wmAct.MasterInfo(name, found, err)
				continue
			}

			l.mi = masterInfo{
				name:  name,
				found: found,
			}

			go l.wmAct.MasterInfo(name, found, err)

		case <-l.stopCheckMaster:
			l.log.Info(
				"leave received",
				zap.String("struct", "Election"),
				zap.String("func", "asyncCheckMaster"),
			)
			return
		}
	}
}

func (l *Election) getMasterInfo() (string, bool, error) {

	kv := l.client.KV()
	opts := &api.QueryOptions{AllowStale: false, RequireConsistent: true}

	pair, _, err := kv.Get(l.settings.Key, opts)

	if err != nil {
		return "", false, err
	}

	if pair == nil {
		return "", false, errors.New("Invalid kv path: " + l.settings.Key)
	}

	if pair.Session == "" {
		return "", false, err
	}

	return string(pair.Value), true, err

}
