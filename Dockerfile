# Supabase-BigQuery Data Pipeline Container
# Multi-stage build for optimized production deployment

FROM python:3.11-slim as builder

# Set environment variables for build
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy dependency files
COPY requirements-bec.yaml .
COPY bec-meltano/meltano.yml ./bec-meltano/
COPY bec_dbt/dbt_project.yml ./bec_dbt/
COPY bec_dbt/profiles.yml ./bec_dbt/

# Install conda/mamba for faster dependency resolution
RUN curl -L https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -o miniforge.sh \
    && bash miniforge.sh -b -p /opt/conda \
    && rm miniforge.sh
ENV PATH="/opt/conda/bin:$PATH"

# Create conda environment from requirements
RUN conda env create -f requirements-bec.yaml
ENV PATH="/opt/conda/envs/bec/bin:$PATH"

# Production stage
FROM python:3.11-slim as production

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PATH="/opt/conda/envs/bec/bin:$PATH"

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy conda environment from builder
COPY --from=builder /opt/conda /opt/conda

# Create app user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=appuser:appuser . .

# Create necessary directories
RUN mkdir -p /app/logs /app/data \
    && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose ports for Dagster web server
EXPOSE 3000

# Health check for web server
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

# Default command - start the Dagster web server
CMD ["python", "start_server.py"]
