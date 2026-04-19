# ──────────────────────────────────────────────────────────────────────────────
# SafeCity SP — Dockerfile do Pipeline PySpark
#
# Por que Python 3.11 e não a versão mais recente?
#   PySpark 3.5.x suporta oficialmente Python 3.8–3.11.
#   Python 3.12+ removeu 'distutils' e alterou o modelo de serialização
#   (cloudpickle), causando RecursionError no PySpark 3.5.x.
#   Python 3.11 é a última versão com suporte garantido.
#
# Por que bitnami/spark em vez de imagem base + instalação manual?
#   A imagem oficial do Spark já inclui Java, Hadoop winutils e todas
#   as variáveis de ambiente configuradas. Evita ~30 linhas de setup.
#   DESVANTAGEM: imagem maior (~1.5GB). Aceitável para uso local.
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# Instalar Java — requisito do PySpark em modo local
# OpenJDK 21 é a versão LTS mais recente com suporte garantido no Debian
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Definir JAVA_HOME — o PySpark lê essa variável para encontrar o JVM
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Diretório de trabalho dentro do container
WORKDIR /app

# Copiar requirements primeiro para aproveitar o cache de layers do Docker.
# Se requirements.txt não mudar, o pip install não é reexecutado em rebuilds.
COPY requirements.txt .

# Instalar dependências Python
# --no-cache-dir: reduz tamanho da imagem (sem cache do pip)
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código do pipeline
COPY . .

# Variável padrão de memória — pode ser sobrescrita pelo docker-compose
ENV SPARK_DRIVER_MEMORY=4g
# Garantir encoding UTF-8 no terminal do container (evita erros com acentos)
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Entry point padrão: pipeline completo
# Pode ser sobrescrito via: docker run ... python ingest.py
CMD ["python", "run_pipeline.py"]
