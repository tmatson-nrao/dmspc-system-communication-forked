FROM chrislusf/seaweedfs:latest

# Copy entrypoint script
COPY entrypoint.sh /seaweedfs.sh
RUN chmod +x /seaweedfs.sh

# Expose ports: 9333 (Master), 8080 (Filer), 8333 (S3)
EXPOSE 9333 8080 8333

ENTRYPOINT ["/seaweedfs.sh"]
