package com.example.demo_colas_azure.service;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;

import lombok.extern.slf4j.Slf4j;

/**
 * Implementación del listener para el perfil {@code single}.
 * Procesa los mensajes de uno en uno ({@code maxConcurrentCalls=1}) manteniendo
 * consumo secuencial y sin prefetch.
 */
@Service
@Profile("single")
@Slf4j
public class SingleQueueListenerManager extends AbstractQueueListenerManager {

    public SingleQueueListenerManager(
            String inputConnectionString,
            String inputQueueName,
            Duration maxAutoLockRenewDuration,
            MessageProcessorService messageProcessorService,
            AmqpRetryOptions amqpRetryOptions) {
        this(inputConnectionString, inputQueueName, maxAutoLockRenewDuration,
                DEFAULT_DRAIN_TIMEOUT, DEFAULT_CLOSE_TIMEOUT,
                messageProcessorService, amqpRetryOptions);
    }

    @Autowired
    public SingleQueueListenerManager(
            @Value("${azure.servicebus.input.connection-string}") String inputConnectionString,
            @Value("${azure.servicebus.input.queue-name}") String inputQueueName,
            @Value("${azure.servicebus.input.max-auto-lock-renew-duration:5m}") Duration maxAutoLockRenewDuration,
            @Value("${azure.servicebus.input.drain-timeout:10s}") Duration drainTimeout,
            @Value("${azure.servicebus.input.close-timeout:5s}") Duration closeTimeout,
            MessageProcessorService messageProcessorService,
            AmqpRetryOptions amqpRetryOptions) {
        super(inputConnectionString, inputQueueName, maxAutoLockRenewDuration,
                drainTimeout, closeTimeout,
                messageProcessorService, amqpRetryOptions);
        log.info("Bean QueueListenerManager inicializado en modo SINGLE (maxConcurrentCalls=1)");
    }

    @Override
    protected void configureProcessor(ServiceBusClientBuilder.ServiceBusProcessorClientBuilder builder) {
        builder.maxConcurrentCalls(1);
    }

    @Override
    protected String getProcessorConfigurationSummary() {
        return String.format("maxConcurrentCalls=1, maxAutoLockRenewDuration=%s", maxAutoLockRenewDuration);
    }

    @Override
    public String getMode() {
        return "single";
    }
}

