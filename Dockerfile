FROM python:3.11-slim-bookworm

# PySpark 3.5 needs a JRE 17+. headless keeps the image small.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# The image is built for whichever arch the host is; resolve JAVA_HOME at runtime rather
# than guessing between arm64 and amd64.
RUN JH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
 && echo "export JAVA_HOME=$JH" > /etc/profile.d/java.sh \
 && ln -sfn "$JH" /opt/java
ENV JAVA_HOME=/opt/java

# Spark resolves the container hostname otherwise and dies with UnresolvedAddressException.
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements-container.txt ./
RUN pip install --no-cache-dir -r requirements-container.txt

COPY src/ ./src/

# Scratch space for Spark spill. Kept on the container filesystem so spill is measured
# against the same disk for both engines.
RUN mkdir -p /tmp/spark /lake /out /results

ENTRYPOINT ["python", "-m"]
CMD ["src.run_engine", "--help"]
