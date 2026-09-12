ARG KESTRA_IMAGE=kestra/kestra:latest
FROM ${KESTRA_IMAGE}

USER root

# L'image Kestra place /app/.venv/bin en tête du PATH. Ce venv interne
# n'embarque pas pip : on cible donc explicitement le Python système.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && /usr/bin/python3 -m pip install --break-system-packages --no-cache-dir \
        duckdb==1.3.2 \
        "pandas>=2.2,<3.0" \
        "openpyxl>=3.1,<4.0" \
        xlwt==1.3.0 \
    && /usr/bin/python3 -c "import duckdb, openpyxl, pandas, xlwt" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app/project/src
