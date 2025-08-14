from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client import Info

# The prometheus metrics registry, which is exposed on the validator api (`vali_utils/api/server.py`)
prometheus_collector_registry = CollectorRegistry()

NAMESPACE = "sn13"
SUBSYSTEM = "validator"


# Buckets for histograms we need have higher duration than typicall web apis
COMMON_HIST_DURATION_BUKCET = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    20.0,
    30.0,
    45.0,
    60.0,
    75.0,
    100.0,
    115.0,
    130.0,
    145.0,
    160.0,
    175.0,
    200.0,
    215.0,
    230.0,
    float("inf"),
)

_params = {
    'registry': prometheus_collector_registry,
    'namespace': NAMESPACE,
    'subsystem': SUBSYSTEM
}

## Static Info ##
# Populated once on setup
VALIDATOR_INFO = Info(
    "validator_info",
    "Static info about this validator",
    **_params
)

## Metrics ##
# Register metrics you wish to track with grafana here and they will automatically show up on /metrics

MAIN_LOOP_ITERATIONS = Counter(
    "main_loop_iterations_total",
    "Total successful main-loop iterations",
    labelnames=["hotkey"],
    **_params

)

MAIN_LOOP_ERRORS = Counter(
    "main_loop_errors_total",
    "Total main-loop iterations that errored",
    labelnames=["hotkey"],
    **_params
)

MAIN_LOOP_DURATION = Gauge(
    "main_loop_duration_seconds",
    "Duration of a main-loop iteration in seconds",
    labelnames=["hotkey"],
    **_params
)

MAIN_LOOP_LAST_SUCCESS_TS = Gauge(
    "main_loop_last_success_timestamp_seconds",
    "Unix timestamp of the last successful iteration",
    labelnames=["hotkey"],
    **_params
)

SET_WEIGHTS_LAST_TS_ATTEMPTED = Gauge(
    "set_weights_last_ts_attempted",
    "Unix timestamp of the last set_weights() attempt",
    labelnames=["hotkey"],
    **_params
)

SET_WEIGHTS_LAST_TS_SUCCESSFUL = Gauge(
    "set_weights_last_ts_successful",
    "Unix timestamp of the last successful set_weights()",
    labelnames=["hotkey"],
    **_params
)

SET_WEIGHTS_SUBTENSOR_DURATION = Histogram(
    "set_weights_subtensor_duration_seconds",
    "Duration of the subtensor.set_weights() call",
    labelnames=["hotkey"],
    buckets=(
        0.01, 0.025, 0.05, 0.075, 0.1,
        0.25, 0.5, 0.75, 1, 1.5, 2, 2.5, 3, 5, 7.5, 10,
        15, 30, 45, 60, 90, 120, 150, 180
    ),
    **_params
)
