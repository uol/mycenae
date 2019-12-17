package tsstats

import (
	"fmt"
	"github.com/uol/gobol/logh"
	"github.com/uol/gobol/snitch"
	"github.com/uol/mycenae/lib/constants"
	"gopkg.in/robfig/cron.v2"
)

const (
	cPackage string = "tsstats"
)

func New(gbs, gbsa *snitch.Stats, intvl, intvla string) (*StatsTS, error) {

	if gbs == nil {
		return nil, fmt.Errorf("stats instance is null")
	}

	if gbsa == nil {
		return nil, fmt.Errorf("analytics stats instance is null")
	}

	if _, err := cron.Parse(intvl); err != nil {
		return nil, err
	}
	return &StatsTS{
		stats:            gbs,
		interval:         intvl,
		analytic:         gbsa,
		analyticInterval: intvla,
	}, nil
}

type StatsTS struct {
	stats            *snitch.Stats
	interval         string
	analytic         *snitch.Stats
	analyticInterval string
}

func (sts *StatsTS) Increment(callerID string, metric string, tags map[string]string) {
	err := sts.stats.Increment(metric, tags, sts.interval, false, true)
	if err != nil {
		if logh.ErrorEnabled {
			logh.Error().Str(constants.StringsPKG, callerID).Str(constants.StringsFunc, "Increment").Str("metric", metric).Err(err).Send()
		}
	}
}

func (sts *StatsTS) ValueAdd(callerID string, metric string, tags map[string]string, v float64) {
	err := sts.stats.ValueAdd(metric, tags, "avg", sts.interval, false, false, v)
	if err != nil {
		if logh.ErrorEnabled {
			logh.Error().Str(constants.StringsPKG, callerID).Str(constants.StringsFunc, "ValueAdd").Str("metric", metric).Err(err).Send()
		}
	}
}

func (sts *StatsTS) ValueMax(callerID string, metric string, tags map[string]string, v float64) {
	err := sts.stats.ValueAdd(metric, tags, "max", sts.interval, false, false, v)
	if err != nil {
		if logh.ErrorEnabled {
			logh.Error().Str(constants.StringsPKG, callerID).Str(constants.StringsFunc, "ValueMax").Str("metric", metric).Err(err).Send()
		}
	}
}

func (sts *StatsTS) AnalyticIncrement(callerID string, metric string, tags map[string]string) {
	err := sts.analytic.Increment(metric, tags, sts.analyticInterval, false, true)
	if err != nil {
		if logh.ErrorEnabled {
			logh.Error().Str(constants.StringsPKG, callerID).Str(constants.StringsFunc, "AnalyticIncrement").Str("metric", metric).Err(err).Send()
		}
	}
}
