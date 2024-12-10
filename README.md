# Schema Migrator

## Overview

This tool is designed to migrate schemas from one Schema Registry to another. It has the following capabilities:

- Read schemas via REST or file (including v1 style exports written by the previous Python tool)
- Write schemas to file or direct to a topic
- Sorting schemas into dependency order (by Schema ID)
- Validate that a registry export is self-consistent (all references are available within the export)
- Validate two registry sources to look for inconsistencies / missing elements by performing a diff

## Compile and Run

```bash
$ go build .
```

```bash
./go-schema-migrator --config config.yaml
```

## Configuration

There are two modes of operation: `migrate` and `validate`. Use `migrate` when copying schemas from a source to a sink.
Use `validate` when comparing two sources to look for inconsistencies.

### Migrate

In migrate mode, specify exactly one source and one sink:

```yaml
action: migrate

source:
  rest:
    ...
  

sink:
  file:
    ...
```

### Validate

```yaml
action: validate

sourceA:
  rest:
    ...
  

sourceB:
  rest:
    ...
```

### Sources

There are three sources available today:

- REST: for connecting to a Schema Registry instance over HTTP
- File: for reading back an intermediate YAML file
- FileV1: for reading an intermediate file produced by the previous Python tool

The following configuration snippets show these sources in use:

```yaml
source:
  rest:
    url: https://schema-registry-redacted.redacted.fmc.prd.cloud.redpanda.com:30081
    username: redacted
    password: redacted
    tls:
      enabled: true
```

```yaml
source:
  file:
    filename: ./registry-export.yaml
```

```yaml
source:
  v1file:
    filename: ./exported.schemas
```

### Sinks

There are three sources available today:

- File: for writing out an intermediate YAML file
- Topic: for writing out messages directly to a `_schemas` topic
- Debug: for console output

The following configuration snippets show these sinks in use:

```yaml
sink:
  file:
    filename: ./registry.yaml
```

```yaml
sink:
  topic:
    seed: seed-redacted.redacted.fmc.prd.cloud.redpanda.com:9092
    topic: _schemas
    compatibility: BACKWARD
    tls:
      enabled: true
    sasl:
      username: redacted
      password: redacted
      mechanism: SCRAM-SHA-256
```

```yaml
sink:
  debug: {}
```

## Use Cases

The following use cases are envisaged:

- Export an existing registry to an intermediate file: [export.yaml](./examples/export.yaml)
- Import an intermediate file to a new registry: [import.yaml](./examples/import.yaml)
- Validate a new registry against an existing registry: [validate.yaml](./examples/validate.yaml)
- Convert a V1 export into V2 (for using in a future import): [convert_v1.yaml](./examples/convert_v1.yaml)

## Possible Future Work

- REST Sink, allowing migration without writing messages to the `_schemas` topic
- Topic Source, allowing an existing `_schemas` topic to be used directly