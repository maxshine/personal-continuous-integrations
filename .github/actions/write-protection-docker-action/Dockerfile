FROM python:3.10.11-slim-bullseye

RUN apt update && apt install -y git curl && apt clean \
    && pip install --upgrade pip virtualenv \
    && curl -sSL https://github.com/maxshine/personal-continuous-integrations/releases/download/v1.3.2/customizable_continuous_integration-1.3.2-py3-none-any.whl -o customizable_continuous_integration-1.3.2-py3-none-any.whl \
    && pip install customizable_continuous_integration-1.3.2-py3-none-any.whl \
    && rm customizable_continuous_integration-1.3.2-py3-none-any.whl

COPY write-protection-pr.sh /usr/local/bin/

ENTRYPOINT ["ci_cli"]
