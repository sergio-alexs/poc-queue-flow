package com.example.demo_colas_azure.service;

import java.time.Duration;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;

/**
 * Implementación del listener para el perfil {@code multiple}.
 * Permite procesar múltiples mensajes en paralelo de forma concurrente
 * configurando {@code maxConcurrentCalls} y {@code prefetchCount}.
 * <p>
 * Se registra como bean mediante {@link com.example.demo_colas_azure.config.MultipleQueueListenerConfig}
 * cuando el perfil {@code multiple} está activo.
 */
public class MultipleQueueListenerManager extends AbstractQueueListenerManager {

    private final int maxConcurrentCalls;
    private final int prefetchCount;

    public MultipleQueueListenerManager(
            String inputConnectionString,
            String inputQueueName,
            Duration maxAutoLockRenewDuration,
            int maxConcurrentCalls,
            int prefetchCount,
            MessageProcessorService messageProcessorService,
            AmqpRetryOptions amqpRetryOptions) {
        this(inputConnectionString, inputQueueName, maxAutoLockRenewDuration,
                DEFAULT_DRAIN_TIMEOUT, DEFAULT_CLOSE_TIMEOUT,
                maxConcurrentCalls, prefetchCount,
                messageProcessorService, amqpRetryOptions);
    }

    public MultipleQueueListenerManager(
            String inputConnectionString,
            String inputQueueName,
            Duration maxAutoLockRenewDuration,
            Duration drainTimeout,
            Duration closeTimeout,
            int maxConcurrentCalls,
            int prefetchCount,
            MessageProcessorService messageProcessorService,
            AmqpRetryOptions amqpRetryOptions) {
        super(inputConnectionString, inputQueueName, maxAutoLockRenewDuration,
                drainTimeout, closeTimeout,
                messageProcessorService, amqpRetryOptions);
        if (maxConcurrentCalls < 1) {
            throw new IllegalArgumentException("'maxConcurrentCalls' debe ser mayor o igual a 1");
        }
        if (prefetchCount < 0) {
            throw new IllegalArgumentException("'prefetchCount' no puede ser negativo");
        }
        this.maxConcurrentCalls = maxConcurrentCalls;
        this.prefetchCount = prefetchCount;
    }

    public int getMaxConcurrentCalls() {
        return maxConcurrentCalls;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }

    @Override
    protected void configureProcessor(ServiceBusClientBuilder.ServiceBusProcessorClientBuilder builder) {
        builder.maxConcurrentCalls(maxConcurrentCalls)
                .prefetchCount(prefetchCount);
    }

    @Override
    protected String getProcessorConfigurationSummary() {
        return String.format("maxConcurrentCalls=%d, prefetchCount=%d, maxAutoLockRenewDuration=%s",
                maxConcurrentCalls, prefetchCount, maxAutoLockRenewDuration);
    }

    @Override
    public String getMode() {
        return "multiple";
    }
}

