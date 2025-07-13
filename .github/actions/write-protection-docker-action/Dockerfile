FROM python:3.12.11-slim-bookworm

RUN apt update && apt install -y git curl && apt clean \
    && pip install --upgrade pip virtualenv \
    && curl -sSL https://github.com/maxshine/personal-continuous-integrations/releases/download/v1.4.3/customizable_continuous_integration-1.4.3-py3-none-any.whl -o customizable_continuous_integration-1.4.3-py3-none-any.whl \
    && pip install customizable_continuous_integration-1.4.3-py3-none-any.whl \
    && rm customizable_continuous_integration-1.4.3-py3-none-any.whl

COPY write-protection-pr.sh /usr/local/bin/

ENTRYPOINT ["ci_cli"]
