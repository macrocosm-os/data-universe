import datadog
import bittensor as bt


def initialize(config: bt.config):
    options = {
        "statsd_host": "127.0.0.1",
        "statsd_port": 8125,
        "statsd_constant_tags": [
            f"subnet:{config.netuid}",
            f"wallet:{config.wallet.hotkey}",
        ],
    }

    datadog.initialize(**options)
