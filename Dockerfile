FROM python:3.10.11-slim-bullseye

ARG POETRY_VERSION=1.8.3
RUN pip install --upgrade pip virtualenv && mkdir /app && python3 -m virtualenv /app/venv
ENV VIRTUAL_ENV="/app/venv"
ENV PATH="/app/venv/bin/:$PATH:/root/.local/bin/"
COPY . /app
RUN apt update && apt install -y git curl && curl -sSL https://install.python-poetry.org | python3 - --version ${POETRY_VERSION}
RUN cd /app && poetry install && cd -

ENTRYPOINT ["ci_cli"]