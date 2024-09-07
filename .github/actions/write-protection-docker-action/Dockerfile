FROM python:3.10.11-slim-bullseye

ARG POETRY_VERSION=1.8.3
RUN apt update && apt install -y git curl \
    && pip install --upgrade pip virtualenv \
    && curl -sSL https://github.com/maxshine/personal-continuous-integrations/releases/download/v1.0.0/customizable_continuous_integration-1.0.0-py3-none-any.whl -o package.whl \
    && pip install package.whl

COPY write-protection-pr.sh /usr/local/bin/

ENTRYPOINT ["ci_cli"]
