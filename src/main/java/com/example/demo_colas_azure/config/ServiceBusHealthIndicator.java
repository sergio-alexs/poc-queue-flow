package com.example.demo_colas_azure.config;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import com.example.demo_colas_azure.service.QueueListenerManager;

import lombok.RequiredArgsConstructor;

/**
 * HealthIndicator personalizado para Kubernetes (Liveness y Readiness Probes).
 * Permite a Kubernetes detectar si el microservicio se ha vuelto un "pod zombie"
 * por deadlocks o errores fatales en las conexiones a Service Bus.
 */
@Component("serviceBus")
@RequiredArgsConstructor
public class ServiceBusHealthIndicator implements HealthIndicator {

    private final QueueListenerManager queueListenerManager;

    @Override
    public Health health() {
        if (queueListenerManager.isFatalErrorEncountered()) {
            return Health.down()
                    .withDetail("reason", "Fallo fatal en conexión o autenticación de Azure Service Bus")
                    .withDetail("error", queueListenerManager.getLastFatalErrorMessage())
                    .withDetail("listenerRunning", false)
                    .build();
        }

        return Health.up()
                .withDetail("listenerRunning", queueListenerManager.isRunning())
                .withDetail("messagesProcessed", queueListenerManager.getMessagesProcessed())
                .withDetail("messagesAbandoned", queueListenerManager.getMessagesAbandoned())
                .withDetail("messagesDeadLettered", queueListenerManager.getMessagesDeadLettered())
                .withDetail("messagesErrored", queueListenerManager.getMessagesErrored())
                .withDetail("lastMessageProcessedAt", queueListenerManager.getLastMessageProcessedAt())
                .build();
    }
}
