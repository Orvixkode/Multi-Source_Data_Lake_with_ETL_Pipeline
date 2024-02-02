# Multi-Source Data Lake - PostgreSQL Database
FROM postgres:15-alpine

# Set environment variables
ENV POSTGRES_DB=datalake
ENV POSTGRES_USER=admin
ENV POSTGRES_PASSWORD=changeme123

# Copy initialization scripts
COPY ./init/ /docker-entrypoint-initdb.d/

# Expose port
EXPOSE 5432