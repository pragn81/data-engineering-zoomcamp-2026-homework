FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root

# Instalar uv y dependencias Python con versiones fijas
RUN pip install --no-cache-dir uv && \
    uv pip install --system --no-cache \
    pandas \
    pyarrow \
    jupyter

WORKDIR /workspace

# Configurar entrypoint para exponer PySpark en Jupyter
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark
ENV HOME=/home/spark

USER spark

ENTRYPOINT ["/entrypoint.sh"]
