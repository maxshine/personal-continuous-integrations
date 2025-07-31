FROM python:3.12.11-slim-bookworm

RUN apt update && apt install -y git curl && apt clean \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && pip install --upgrade pip virtualenv 
RUN curl -sSL https://github.com/maxshine/personal-continuous-integrations/releases/download/v1.4.4/customizable_continuous_integration-1.4.4-py3-none-any.whl -o customizable_continuous_integration-1.4.4-py3-none-any.whl \
    && $HOME/.local/bin/uv pip install --system customizable_continuous_integration-1.4.4-py3-none-any.whl \
    && rm customizable_continuous_integration-1.4.4-py3-none-any.whl
COPY write-protection-pr.sh /usr/local/bin/
ENTRYPOINT ["ci_cli"]
