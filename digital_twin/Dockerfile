FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install -r requirements.txt

ENV CONFIG_PATH=/app/dt/configs/config.yaml
ENV PYTHONUNBUFFERED=1

COPY ./main.py /app
COPY ./connections.py /app
COPY ./process.py /app
COPY ./events.py /app

ENTRYPOINT ["python3"]
CMD ["main.py"]