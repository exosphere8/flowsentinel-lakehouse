# syntax=docker/dockerfile:1.7
# FlowSentinel Lakehouse: the pipeline, Dagster and the FlowSentinel CLI in one image.
# The self-hosted suite in deploy/ runs it next to FlowSentinel's own server and dashboard.
#
#   docker build -t flowsentinel-lakehouse .
#
# Behind a TLS-inspecting proxy, give the build the proxy's CA certificate:
#   docker build --secret id=extra_ca,src=proxy-ca.crt -t flowsentinel-lakehouse .

# The FlowSentinel commit whose output format this lakehouse is tested against (see CI).
ARG FLOWSENTINEL_REPO=https://github.com/exosphere8/flowsentinel.git
ARG FLOWSENTINEL_REF=dfdb3cba20f031187bbc867124cd90af5f98a3db

# ---------------------------------------------------------------- the flowsentinel CLI
FROM rust:1.97-slim-trixie AS flowsentinel
ARG FLOWSENTINEL_REPO
ARG FLOWSENTINEL_REF
# BuildKit fetches the repository itself, so the image needs no git and no package manager.
ADD ${FLOWSENTINEL_REPO}#${FLOWSENTINEL_REF} /src
WORKDIR /src
RUN --mount=type=secret,id=extra_ca,required=false \
    set -eu; \
    if [ -s /run/secrets/extra_ca ]; then \
        cp /run/secrets/extra_ca /usr/local/share/ca-certificates/extra-ca.crt; \
        update-ca-certificates >/dev/null 2>&1; \
    fi; \
    cargo build --release --locked -p cli

# ---------------------------------------------------------------- the Python environment
FROM python:3.12-slim-trixie AS venv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /build
RUN --mount=type=secret,id=extra_ca,required=false \
    if [ -s /run/secrets/extra_ca ]; then export PIP_CERT=/run/secrets/extra_ca; fi; \
    pip install --no-cache-dir uv==0.8.17
# Dependencies first, at the exact versions in uv.lock, so a code change does not reinstall them.
COPY pyproject.toml uv.lock ./
RUN --mount=type=secret,id=extra_ca,required=false \
    if [ -s /run/secrets/extra_ca ]; then export SSL_CERT_FILE=/run/secrets/extra_ca; fi; \
    uv sync --frozen --no-dev --all-extras --no-install-project
# Then the project itself, as a package with the dbt project inside.
COPY README.md LICENSE ./
COPY src ./src
COPY transform ./transform
RUN --mount=type=secret,id=extra_ca,required=false \
    if [ -s /run/secrets/extra_ca ]; then export SSL_CERT_FILE=/run/secrets/extra_ca; fi; \
    uv sync --frozen --no-dev --all-extras --no-editable

# ---------------------------------------------------------------- the lakehouse
FROM python:3.12-slim-trixie
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
COPY --from=venv /opt/venv /opt/venv
COPY --from=flowsentinel /src/target/release/flowsentinel /usr/local/bin/flowsentinel
COPY deploy/docker/entrypoint.sh /usr/local/bin/flowlake-entrypoint
COPY deploy/docker/dagster.yaml /opt/flowlake/dagster.yaml
COPY LICENSE /usr/share/doc/flowsentinel-lakehouse/LICENSE
RUN set -eu; \
    useradd --system --uid 10001 --user-group --home-dir /data --shell /usr/sbin/nologin flowlake; \
    install -d -o 10001 -g 10001 -m 0755 /data /data/lake /data/site /data/dagster /data/inbox /data/tmp /config
ENV PATH=/opt/venv/bin:$PATH \
    HOME=/tmp \
    FLOWLAKE_LAKE=/data/lake \
    FLOWLAKE_LANDING=/data/inbox \
    FLOWLAKE_SITE=/data/site \
    TMPDIR=/data/tmp \
    DAGSTER_HOME=/data/dagster \
    FLOWSENTINEL_BIN=/usr/local/bin/flowsentinel \
    DO_NOT_TRACK=1
USER 10001:10001
WORKDIR /data
EXPOSE 3000
ENTRYPOINT ["flowlake-entrypoint"]
CMD ["dagster-webserver", "--host", "0.0.0.0", "--port", "3000", "-m", "flowlake.orchestration"]
