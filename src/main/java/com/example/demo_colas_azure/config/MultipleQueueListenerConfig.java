package com.example.demo_colas_azure.config;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.azure.core.amqp.AmqpRetryOptions;
import com.example.demo_colas_azure.service.MessageProcessorService;
import com.example.demo_colas_azure.service.MultipleQueueListenerManager;
import com.example.demo_colas_azure.service.QueueListenerManager;

import lombok.extern.slf4j.Slf4j;

/**
 * Configuración activada bajo el perfil {@code multiple} para registrar
 * la implementación concurrente de {@link QueueListenerManager}.
 */
@Configuration
@Profile("multiple")
@Slf4j
public class MultipleQueueListenerConfig {

    @Bean
    public QueueListenerManager queueListenerManager(
            @Value("${azure.servicebus.input.connection-string}") String inputConnectionString,
            @Value("${azure.servicebus.input.queue-name}") String inputQueueName,
            @Value("${azure.servicebus.input.max-auto-lock-renew-duration:5m}") Duration maxAutoLockRenewDuration,
            @Value("${azure.servicebus.input.drain-timeout:10s}") Duration drainTimeout,
            @Value("${azure.servicebus.input.close-timeout:5s}") Duration closeTimeout,
            @Value("${azure.servicebus.input.multiple.max-concurrent-calls:10}") int maxConcurrentCalls,
            @Value("${azure.servicebus.input.multiple.prefetch-count:20}") int prefetchCount,
            MessageProcessorService messageProcessorService,
            AmqpRetryOptions amqpRetryOptions) {
        log.info("Registrando bean QueueListenerManager en modo MULTIPLE (maxConcurrentCalls={}, prefetchCount={}, drainTimeout={}, closeTimeout={})",
                maxConcurrentCalls, prefetchCount, drainTimeout, closeTimeout);
        return new MultipleQueueListenerManager(
                inputConnectionString,
                inputQueueName,
                maxAutoLockRenewDuration,
                drainTimeout,
                closeTimeout,
                maxConcurrentCalls,
                prefetchCount,
                messageProcessorService,
                amqpRetryOptions);
    }
}

