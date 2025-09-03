from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client import Info

# The prometheus metrics registry, which is exposed on the validator api (`vali_utils/api/server.py`)
prometheus_collector_registry = CollectorRegistry()

NAMESPACE = "sn13"
SUBSYSTEM = "validator"


# Buckets for histograms we need have higher duration than typicall web apis
COMMON_LIVE_REQUEST_HIST_DURATION_BUCKET = (
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
)

_params = {
    "registry": prometheus_collector_registry,
    "namespace": NAMESPACE,
    "subsystem": SUBSYSTEM,
}

## Static Info ##
# Populated once on setup
VALIDATOR_INFO = Info("validator_info", "Static info about this validator", **_params)

## Metrics ##
# Register metrics you wish to track with grafana here and they will automatically show up on /metrics

## Main Loop
MAIN_LOOP_ITERATIONS_TOTAL = Counter(
    "main_loop_iterations_total",
    "Total successful main-loop iterations",
    labelnames=["hotkey"],
    **_params,
    unit="iterations",
)

MAIN_LOOP_ERRORS_TOTAL = Counter(
    "main_loop_errors_total",
    "Total main-loop iterations that errored",
    labelnames=["hotkey"],
    **_params,
)

MAIN_LOOP_DURATION = Gauge(
    "main_loop_duration",
    "Duration of a main-loop iteration",
    labelnames=["hotkey"],
    **_params,
    unit="seconds",
)

MAIN_LOOP_LAST_SUCCESS_TS = Gauge(
    "main_loop_last_success_ts",
    "Unix timestamp of the last successful iteration",
    labelnames=["hotkey"],
    **_params,
    unit="seconds",
)

## Weight setting
SET_WEIGHTS_LAST_TS = Gauge(
    "set_weights_last_ts",
    "Unix timestamp of the last set_weights()",
    labelnames=["hotkey", 'status'],
    **_params,
    unit="seconds",
)

SET_WEIGHTS_SUBTENSOR_DURATION = Histogram(
    "set_weights_subtensor_duration",
    "Duration of the subtensor.set_weights() call",
    labelnames=["hotkey"],
    buckets=COMMON_LIVE_REQUEST_HIST_DURATION_BUCKET,
    **_params,
    unit="seconds",
)

## Organic Query - Synapse Handler
ORGANIC_QUERY_PROCESS_DURATION = Histogram(
    "organic_query_process_duration",
    "Duration of Validator.process_organic_query synapse",
    labelnames=["request_source", "response_status"],
    buckets=COMMON_LIVE_REQUEST_HIST_DURATION_BUCKET,
    **_params,
    unit="seconds",
)

ORGANIC_QUERY_RESPONSE_SIZE = Histogram(
    "organic_query_response_size",
    "Data size in bytes (json dump) of Validator.process_organic_query synapse response",
    labelnames=["request_source", "response_status"],
    buckets=[0, 200, 400, 600, 800, 1000, 1200, 1500, 2000, 5000, 10000],
    **_params,
    unit="bytes",
)

ORGANIC_QUERY_REQUESTS_TOTAL = Counter(
    "organic_query_requests_total",
    "Total number of organic query requests",
    labelnames=["request_source", "response_status"],
    **_params,
    unit="requests",
)

ORGANIC_MINER_FAILURES = Counter(
    "organic_miner_failures",
    "Total number of organic query requests",
    labelnames=["miner_uid", "failure_type"],
    **_params,
    unit="failures",
)


## Dynamic Desirability
DYNAMIC_DESIRABILITY_RETRIEVAL_LAST_SUCCESSFUL_TS = Gauge(
    "dynamic_desirability_last_successful_ts",
    "Last successful update of dynamic desirability list retrieval",
    labelnames=["hotkey"],
    **_params,
    unit="seconds",
)

DYNAMIC_DESIRABILITY_RETRIEVAL_PROCESS_DURATION = Gauge(
    "dynamic_desirability_retrieval_process_duration",
    "Duration of Validator.process_organic_query synapse",
    labelnames=["hotkey"],
    **_params,
    unit="seconds",
)

## Metagraph Updates and On-Chain Health Checks
BITTENSOR_VALIDATOR_REGISTERED = Gauge(
    "bittensor_validator_registered",
    "1 if this validator hotkey is registered else 0",
    labelnames=["hotkey"],
    **_params,
    unit="boolean",
)

BITTENSOR_VALIDATOR_VTRUST = Gauge(
    "bittensor_validator_vtrust",
    "Validator vTrust for validator",
    labelnames=["hotkey"],
    **_params,
    unit="ratio",
)

BITTENSOR_VALIDATOR_BLOCK_DIFFERENCE = Gauge(
    "bittensor_validator_block_difference",
    "metagraph.block - metagraph.last_update[uid] for this validator.",
    labelnames=["hotkey"],
    **_params,
)

METAGRAPH_LAST_UPDATE_TS = Gauge(
    "metagraph_last_update_ts",
    "Last timestamp of Validator._on_metagraph_updated call",
    labelnames=["hotkey"],
    **_params,
    unit="seconds",
)

## API - OnDemand
ON_DEMAND_VALIDATOR_QUERY_ATTEMPTS = Histogram(
    "on_demand_validator_query_attempts",
    "Amount of validator query attempts (per request)",
    buckets=(1, 2, 3, 4, 5, 10, 20, 50, 100, 200),
    **_params,
    unit="attempts",
)

ON_DEMAND_VALIDATOR_QUERY_DURATION = Histogram(
    "on_demand_validator_query_duration",
    "Validator query latency (multiple attempts per request)",
    labelnames=["hotkey", "status"],
    buckets=COMMON_LIVE_REQUEST_HIST_DURATION_BUCKET,
    **_params,
    unit="seconds",
)

## Miner Evaluator
COMMON_EVALUATION_HIST_DURATION_BUCKET = tuple(
    x * 60 for x in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,20,30,60)
)

MINER_EVALUATOR_EVAL_BATCH_DURATION = Histogram(
    "miner_evaluator_eval_batch_duration",
    "Total duration of the run_next_eval_batch function which evaluates a mini batch of miners in parallel - only when an update is due",
    labelnames=["hotkey"],
    buckets=COMMON_EVALUATION_HIST_DURATION_BUCKET,
    **_params,
    unit="seconds",
)

MINER_EVALUATOR_EVAL_MINER_DURATION = Histogram(
    "miner_evaluator_eval_miner_duration",
    "Total duration of the the evaluation loop of 1 miner",
    labelnames=["hotkey", "miner_hotkey", "status"],
    buckets=COMMON_EVALUATION_HIST_DURATION_BUCKET,
    **_params,
    unit="seconds",
)

## Dendrites
# DENDRITE_CALL_DURATION = Gauge(
#     "dendrite_call_duration",
#     "Duration of a dendrite.forward",
#     labelnames=["hotkey", "axon_type", "synapse_type", "response_status"],
#     **_params,
#     unit="seconds",
# )
