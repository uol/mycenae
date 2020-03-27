package udp

import "github.com/uol/mycenae/lib/constants"

func (us *UDPserver) statsNetworkConnection(function string) {

	us.timelineManager.FlattenCountIncN(
		function,
		constants.StringsMetricNetworkConnection,
		constants.StringsSource, constants.StringsUDP,
	)
}
