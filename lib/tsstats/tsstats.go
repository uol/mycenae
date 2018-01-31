package tsstats

import (
	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol/snitch"
	"gopkg.in/robfig/cron.v2"
)

func New(gbl *logrus.Logger, gbs *snitch.Stats, intvl, statsKeySet, statsTTL string) (*StatsTS, error) {
	if _, err := cron.Parse(intvl); err != nil {
		return nil, err
	}
	return &StatsTS{
		log:         gbl,
		stats:       gbs,
		interval:    intvl,
		StatsKeySet: statsKeySet,
		StatsTTL:    statsTTL,
	}, nil
}

type StatsTS struct {
	stats       *snitch.Stats
	log         *logrus.Logger
	interval    string
	StatsKeySet string
	StatsTTL    string
}

func (sts *StatsTS) Increment(callerID string, metric string, tags map[string]string) {
	err := sts.stats.Increment(metric, tags, sts.interval, false, true)
	if err != nil {
		sts.log.WithFields(logrus.Fields{
			"package": callerID,
			"func":    "statsIncrement",
			"metric":  metric,
		}).Error(err)
	}
}

func (sts *StatsTS) ValueAdd(callerID string, metric string, tags map[string]string, v float64) {
	err := sts.stats.ValueAdd(metric, tags, "avg", sts.interval, false, false, v)
	if err != nil {
		sts.log.WithFields(logrus.Fields{
			"package": callerID,
			"func":    "statsValueAdd",
			"metric":  metric,
		}).Error(err)
	}
}
