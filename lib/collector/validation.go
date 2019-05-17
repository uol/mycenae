package collector

import (
	"fmt"
	"strconv"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logs a point to the error log
func (collector *Collector) logPointError(p *TSDBpoint, err error, lf []zapcore.Field) {

	jsonStr, errj := json.Marshal(&p)

	if errj != nil {
		gblog.Error(fmt.Sprintf("point error (%+v): %s", &p, err.Error()), lf...)
	} else {
		gblog.Error(fmt.Sprintf("point error (%s): %s", jsonStr, err.Error()), lf...)
	}
}

// Validates a point and fills the packet
func (collector *Collector) MakePacket(packet *Point, rcvMsg TSDBpoint, number bool) gobol.Error {

	lf := []zapcore.Field{
		zap.String("package", "collector"),
		zap.String("func", "makePacket"),
	}

	if number {
		if rcvMsg.Value == nil {
			err := errValidation(`Wrong Format: Field "value" is required. NO information will be saved`)
			collector.logPointError(&rcvMsg, err, lf)
			return err
		}
	} else {
		if rcvMsg.Text == "" {
			err := errValidation(`Wrong Format: Field "text" is required. NO information will be saved`)
			collector.logPointError(&rcvMsg, err, lf)
			return err
		}

		if len(rcvMsg.Text) > 10000 {
			err := errValidation(`Wrong Format: Field "text" can not have more than 10k`)
			collector.logPointError(&rcvMsg, err, lf)
			return err
		}
	}

	lt := len(rcvMsg.Tags)

	if lt == 0 {
		err := errValidation(`Wrong Format: At least one tag is required. NO information will be saved`)
		collector.logPointError(&rcvMsg, err, lf)
		return err
	}

	if !collector.validKey.MatchString(rcvMsg.Metric) {
		err := errValidation(
			fmt.Sprintf(
				`Wrong Format: Field "metric" (%s) is not well formed. NO information will be saved`,
				rcvMsg.Metric,
			),
		)
		collector.logPointError(&rcvMsg, err, lf)
		return err
	}

	if keyset, ok := rcvMsg.Tags["ksid"]; !ok {
		err := errValidation(`Wrong Format: Tag "ksid" is required. NO information will be saved`)
		collector.logPointError(&rcvMsg, err, lf)
		return err
	} else {
		packet.Keyset = keyset
	}

	if !collector.keySet.IsKeySetNameValid(packet.Keyset) {
		err := errValidation(
			fmt.Sprintf(
				`Wrong Format: Field "ksid" (%s) is not well formed. NO information will be saved`,
				packet.Keyset,
			),
		)
		collector.logPointError(&rcvMsg, err, lf)
		return err
	}

	keySetExists, gerr := collector.persist.metaStorage.CheckKeySet(packet.Keyset)
	if gerr != nil {
		err := errISE("makePacket", "error checking keyspace existence", gerr)
		collector.logPointError(&rcvMsg, err, lf)
		return err
	}
	if !keySetExists {
		err := errValidation(`"ksid" not exists. NO information will be saved`)
		collector.logPointError(&rcvMsg, err, lf)
		return err
	}

	if strTTL, ok := rcvMsg.Tags["ttl"]; !ok {
		packet.TTL = collector.settings.DefaultTTL
		lt++
	} else {
		ttl, err := strconv.Atoi(strTTL)
		if err != nil {
			err := errValidation(`Wrong Format: Tag "ttl" must be a positive number. NO information will be saved`)
			collector.logPointError(&rcvMsg, err, lf)
			return err
		}
		if _, ok := collector.keyspaceTTLMap[ttl]; !ok {
			ttl = collector.settings.DefaultTTL
		}
		packet.TTL = ttl
	}
	rcvMsg.Tags["ttl"] = strconv.Itoa(packet.TTL)

	if lt == 2 {
		err := errValidation(`Wrong Format: At least one tag other than "ksid" and "ttl" is required. NO information will be saved`)
		collector.logPointError(&rcvMsg, err, lf)
		return err
	}

	for k, v := range rcvMsg.Tags {
		if !collector.validKey.MatchString(k) {
			err := errValidation(
				fmt.Sprintf(
					`Wrong Format: Tag key (%s) is not well formed. NO information will be saved`,
					k,
				),
			)
			collector.logPointError(&rcvMsg, err, lf)
			return err
		}
		if !collector.validKey.MatchString(v) {
			err := errValidation(
				fmt.Sprintf(
					`Wrong Format: Tag value (%s) is not well formed. NO information will be saved`,
					v,
				),
			)
			collector.logPointError(&rcvMsg, err, lf)
			return err
		}
	}

	if rcvMsg.Timestamp == 0 {
		packet.Timestamp = utils.GetTimeNoMillis()
	} else {
		truncated, err := utils.MilliToSeconds(rcvMsg.Timestamp)
		if err != nil {
			err := errBR("makePacket", err.Error(), err)
			collector.logPointError(&rcvMsg, err, lf)
			return err
		}
		packet.Timestamp = truncated
	}

	packet.Number = number
	packet.Message = rcvMsg
	packet.ID = GenerateID(rcvMsg)
	if !number {
		packet.ID = fmt.Sprintf("T%v", packet.ID)
	}

	return nil
}
