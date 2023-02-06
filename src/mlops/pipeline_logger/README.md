## Pipeline Logger

The pipeline logger can be used throughout the pipeline to log progress, information, errors, and anything else that is required by the user. It based on the basic python logger but builds additional handlers in order to push logs to stdout and to Loki (and subsequently Grafana) if these services are running in the pipeline.

The logging to Loki occurs automatically and the Grafana frontend is the easiest place to access and view the generated logs. Loki and Grafana are enabled in the pipeline set up automatically, with Loki being reachable within the docker compose network at 3100.

The pipeline logger can be called without any arguments. However, it is recommended to pass the name of the service or code calling the logger in order to get better clarity of logging in Loki. The level of logs seen both in stdout and in Grafana through Loki depends on whether or not the verbose flag is passed to the logger. If no flag is passed, the default is `False`, and debug and info messages will not be shown

### Code Example

The following code snippet creates a `PipelineLogger` for the *ingestion* phase of the pipeline. It will log all message levels to stdout and to Loki.

```
from mlops.pipeline_logger import PipelineLogger

def logger_example():
    new_logger = PipelineLogger("ingestion", True)
    new_logger.info("Now you can log to Loki!")
```

