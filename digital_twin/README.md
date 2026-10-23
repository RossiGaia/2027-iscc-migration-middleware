# Digital twin

This component can be run with Python or Docker.

## Config file

Before running the Digital Twin, edit the `config.yaml`.

Example configuration:

```yaml
name: "DT1_ABCDEF_d"

connections:
  mqtt:
    broker_url: "localhost"
    port: 1883
    topics:
    - "test/#"
    - "control"
  mongodb:
    use: no
    url: "mongodb://user:pass@localhost:27017"
    database: "pt"
    collection: "events"

  buffer:
    size: 100

process:
  buffer:
    size: 100
  burn:
    workers: 0
    work: 1000
  file_checkpoints:
    use: no
    interval: 20
    path: "/app/dt/dump/dump.json"

state:
  max_size_mb: 10

flask:
  port: 5003

migration:
  live:
    rounds: 10

logger:
  level:
    shell: debug
    file: debug
    mongo: debug
```

> Make sure the configuration points to the correct MQTT broker and, if enabled, to the correct MongoDB instance.

## Python run

Install the required Python packages:

```bash
pip install -r requirements.txt
```

Run the component with:

```bash
python3 main.py
```

## Dockerize

The Dockerfile is provided as `Dockerfile`.
Build the image:

```bash
docker build -t digital-twin .
```

Run the container:

```bash
docker run --name dt -p <port_in_config>:<port_in_config> digital-twin
```

For example, if flask.port in config.yaml is set to 5003:

```bash
docker run --name dt -p 5003:5003 digital-twin
```