FROM python:3.12

WORKDIR /app

COPY requirements.txt /app
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install -r requirements.txt

COPY ./main.py /app
COPY ./connections.py /app
COPY ./process.py /app

ENTRYPOINT ["python3"]
CMD ["main.py"]