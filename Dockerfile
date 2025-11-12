
ARG AIRFLOW_IMAGE=apache/airflow:2.9.3
FROM ${AIRFLOW_IMAGE}

# Allow builds to run package updates only when apt-get is available. This helps
# reduce some OS-level vulnerabilities while remaining compatible with different
# base images.
USER root

# Copy project into the image and set ownership to the airflow user. Use --chown
# to avoid an extra chown layer.
COPY --chown=airflow:airflow . /opt/airflow/project

# If the image is Debian/Ubuntu based (has apt-get), update system packages to
# pick up security fixes. If apt-get is missing, this step is skipped.
# RUN if command -v apt-get >/dev/null 2>&1; then \
#       apt-get update && apt-get -y upgrade && \
#       apt-get -y install --no-install-recommends build-essential gcc libpq-dev && \
#       apt-get -y autoremove && apt-get clean && rm -rf /var/lib/apt/lists/*; \
#     else \
#       echo "apt-get not found, skipping OS package update"; \
#     fi

# Run as the official airflow user for safety
USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/project/requirements.txt

WORKDIR /opt/airflow

LABEL maintainer="you@example.com"
