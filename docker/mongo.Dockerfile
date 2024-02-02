# Multi-Source Data Lake - MongoDB Database
FROM mongo:7.0

# Set environment variables
ENV MONGO_INITDB_ROOT_USERNAME=admin
ENV MONGO_INITDB_ROOT_PASSWORD=changeme123
ENV MONGO_INITDB_DATABASE=datalake

# Copy initialization scripts
COPY ./init/mongo-init.js /docker-entrypoint-initdb.d/

# Expose port
EXPOSE 27017