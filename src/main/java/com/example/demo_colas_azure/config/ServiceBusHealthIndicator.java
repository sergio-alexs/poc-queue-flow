package com.example.demo_colas_azure.config;

import java.time.Instant;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import com.example.demo_colas_azure.service.QueueListenerManager;

import lombok.RequiredArgsConstructor;

/**
 * HealthIndicator personalizado para Kubernetes (Liveness y Readiness Probes).
 * Permite a Kubernetes detectar si el microservicio se ha vuelto un "pod
 * zombie"
 * por deadlocks o errores fatales en las conexiones a Service Bus.
 */
@Component("serviceBus")
@RequiredArgsConstructor
public class ServiceBusHealthIndicator implements HealthIndicator {

    private final QueueListenerManager queueListenerManager;

    @Override
    public Health health() {
        String mode = queueListenerManager.getMode();

        if (queueListenerManager.isFatalErrorEncountered()) {
            Health.Builder builder = Health.down()
                    .withDetail("reason", "Fallo fatal en conexión o autenticación de Azure Service Bus");

            if (mode != null) {
                builder.withDetail("mode", mode);
            }

            String lastFatalErrorMessage = queueListenerManager.getLastFatalErrorMessage();
            if (lastFatalErrorMessage != null) {
                builder.withDetail("error", lastFatalErrorMessage);
            }

            return builder
                    .withDetail("listenerRunning", false)
                    .withDetail("inFlightMessages", queueListenerManager.getInFlightMessages())
                    .build();
        }

        Health.Builder builder = Health.up()
                .withDetail("listenerRunning", queueListenerManager.isRunning());

        if (mode != null) {
            builder.withDetail("mode", mode);
        }

        builder.withDetail("inFlightMessages", queueListenerManager.getInFlightMessages())
                .withDetail("messagesProcessed", queueListenerManager.getMessagesProcessed())
                .withDetail("messagesAbandoned", queueListenerManager.getMessagesAbandoned())
                .withDetail("messagesDeadLettered", queueListenerManager.getMessagesDeadLettered())
                .withDetail("messagesErrored", queueListenerManager.getMessagesErrored());

        Instant lastMessageProcessedAt = queueListenerManager.getLastMessageProcessedAt();
        if (lastMessageProcessedAt != null) {
            builder.withDetail("lastMessageProcessedAt", lastMessageProcessedAt);
        }

        return builder.build();
    }
}
