
FROM apache/airflow:3.1.2

# Allow builds to run package updates only when apt-get is available. This helps
# reduce some OS-level vulnerabilities while remaining compatible with different
# base images.
USER root

# Copy project into the image and set ownership to the airflow user. Use --chown
# to avoid an extra chown layer.
COPY --chown=airflow:airflow . /opt/airflow/project


# Run as the official airflow user for safety
USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/project/requirements.txt

WORKDIR /opt/airflow

LABEL maintainer="you@example.com"
