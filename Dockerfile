# Use RabbitMQ 4.1.3 with management plugin
FROM rabbitmq:4.1.3-management

# Set environment variables
ENV RABBITMQ_HOME=/opt/rabbitmq \
    RABBITMQ_ENABLED_PLUGINS_FILE=/etc/rabbitmq/enabled_plugins

# Create plugins directory if not exists
RUN mkdir -p /opt/rabbitmq/plugins

# Copy your custom plugin (.ez file) into the container
COPY compiled/1.0.6/rabbitmq_4_1_3/rabbitmq_stamp-1.0.6.ez /opt/rabbitmq/plugins/

# Make sure RabbitMQ knows about the plugin path
ENV RABBITMQ_PLUGIN_PATH="/opt/rabbitmq/plugins:${RABBITMQ_HOME}/plugins"

# Enable the plugin (replace 'rabbitmq_stamp' with actual plugin name)
RUN echo '[rabbitmq_management, rabbitmq_stamp].' > /etc/rabbitmq/enabled_plugins
