## Source Log Document Description

The application log entries originate from a Kubernetes-based environment and are formatted as JSON objects. The extraction service handles **two distinct log formats**:

### Format 1: Application Logs (Structured)
Standard application logs with nested `logs` substructure containing parsed log content.

### Format 2: Container Logs (Direct)
Container/infrastructure logs with direct field mapping, without the nested `logs` structure.

---

## Format 1: Application Logs Structure

### Top-Level Fields

* `@timestamp`: ISO 8601 timestamp string of the log event.
* `record_date`: A date string in `yyyyMMdd` format.
* `stream`: Typically `stdout` or `stderr`.
* `time`: Nanosecond-precision timestamp.
* `_p`: Miscellaneous string marker (e.g., `F`).
* `log`: Stringified JSON version of `logs`, often redundant.
* `logs`: **Object containing the actual structured log content**.
* `kubernetes`: Detailed Kubernetes metadata for the pod/container emitting the log.

### `logs` Substructure

* `instant.epochSecond`: Seconds since UNIX epoch.
* `instant.nanoOfSecond`: Nanosecond portion of the timestamp.
* `timeMillis`: Alternative timestamp format (milliseconds since epoch, for access logs).
* `level`: Log level (e.g., `INFO`, `WARNING`, `ERROR`).
* `loggerName`: Java class name or component.  
* `thread`: Thread name.
* `message`: Log message.
* `thrown` (optional): Exception or error structure, potentially with a full stack trace.
* `contextMap`: Optional map of MDC (Mapped Diagnostic Context) entries.

---

## Format 2: Container Logs Structure

Container logs have a flatter structure without the nested `logs` object:

### Direct Fields

* `@timestamp`: ISO 8601 timestamp string (primary timestamp source).
* `time`: Alternative ISO 8601 timestamp (fallback if @timestamp missing).
* `stream`: Typically `stdout` or `stderr`.
* `log`: **Direct log message content** (not stringified JSON).
* `_p`: Process marker.
* `kubernetes`: Kubernetes metadata (same as Format 1).
* `record_date`: Date string in `yyyyMMdd` format.

**Key Difference**: No nested `logs` substructure - all log content is at the top level.

---

## Extraction Logic

The extractor automatically detects and handles both formats:

```go
if rawLog.Logs != nil {
    // Format 1: Application logs with logs substructure
    return extractFromApplicationLog(&rawLog, rawLine, source)
} else {
    // Format 2: Container logs without logs substructure  
    return extractFromContainerLog(&rawLog, rawLine, source)
}
```

### Format 1 Extraction (Application Logs)
- **Timestamp**: Extracted from `logs.instant.{epochSecond, nanoOfSecond}` or `logs.timeMillis`
- **Level**: From `logs.level` (with fallback to `logs.origin` for access logs)
- **Logger**: From `logs.loggerName` (with fallback to `logs.origin`)
- **Message**: From `logs.message` (with fallback to `logs.contextMap.requestLine`)
- **Thread**: From `logs.thread`  
- **Exception**: From `logs.thrown` (structured)

### Format 2 Extraction (Container Logs)
- **Timestamp**: Parsed from `@timestamp` or `time` fields (ISO 8601 format)
- **Level**: Extracted from log message content (INFO, ERROR, WARN, etc.)
- **Logger**: Set to container name or "container" as fallback
- **Message**: Direct from `log` field
- **Thread**: Not available (empty string)
- **Exception**: Not available (structured exceptions not present)

## Extracted Log Document Model

The extracted document is structured to retain essential information for efficient storage and downstream processing. The format is designed to support Avro encoding and optionally compressed transport (e.g., Kafka).

### Extracted Fields

* `ts_ns` *(int64)*: Derived timestamp in nanoseconds (epochSecond \* 1e9 + nanoOfSecond).
* `level` *(enum/int)*: Numeric code representing log level (e.g., 0=TRACE, 1=DEBUG, 2=INFO, etc.).
* `logger` *(string)*: Fully qualified logger name.
* `thread` *(string)*: Thread name where the log originated.
* `msg` *(string)*: The main log message.
* `thrown` *(optional string)*: Serialized exception stack trace, if present.

## Extraction Code (Python + jq Workflow)

### Step 1: Use `jq` to extract and normalize the content:

```bash
jq -c 'select(.logs != null) | {
  ts_ns: (.logs.instant.epochSecond * 1000000000 + .logs.instant.nanoOfSecond // 0),
  level: .logs.level,
  logger: .logs.loggerName,
  thread: .logs.thread,
  msg: .logs.message,
  thrown: (.logs.thrown // null)
}' app.log > app_extracted_full.log
```

*Note: `// null` or `// 0` is used to avoid errors in case of missing values.*

### Step 2: Optional Python script to convert to Avro

```python
import json
from fastavro import writer, parse_schema

schema = {
    "type": "record",
    "name": "LogEntry",
    "fields": [
        {"name": "ts_ns", "type": "long"},
        {"name": "level", "type": "string"},  # Or use enum or int
        {"name": "logger", "type": "string"},
        {"name": "thread", "type": "string"},
        {"name": "msg", "type": "string"},
        {"name": "thrown", "type": ["null", "string"], "default": None}
    ]
}

parsed_schema = parse_schema(schema)

with open("app_extracted_full.log", "r") as f_in, open("app_extracted_full.avro", "wb") as f_out:
    records = (json.loads(line) for line in f_in)
    writer(f_out, parsed_schema, records)
```

## Additional Notes

* Ensure log levels are normalized (string or enum) before schema enforcement.
* Missing timestamps (`logs.instant`) should not be discarded; fallbacks (like `timeMillis` or `@timestamp`) may be implemented.
* Kubernetes metadata is omitted in this extraction for size reasons but may be handled separately.
