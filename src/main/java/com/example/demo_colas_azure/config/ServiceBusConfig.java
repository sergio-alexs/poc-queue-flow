package com.example.demo_colas_azure.config;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusSenderClient;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuración manual de beans de Azure Service Bus con políticas de reintentos AMQP
 * y cierre controlado con timeout para prevenir Pods zombies en Kubernetes.
 */
@Configuration
@Slf4j
public class ServiceBusConfig {

    @Value("${azure.servicebus.input.sender.connection-string}")
    private String inputSenderConnectionString;

    @Value("${azure.servicebus.input.queue-name}")
    private String inputQueueName;

    @Value("${azure.servicebus.output.connection-string}")
    private String outputConnectionString;

    @Value("${azure.servicebus.output.queue-name}")
    private String outputQueueName;

    private ServiceBusSenderClient outputSenderClient;
    private ServiceBusSenderClient inputSenderClient;

    /**
     * Bean compartido de opciones de reintentos con backoff exponencial
     * para operaciones de red y transporte AMQP.
     */
    @Bean
    public AmqpRetryOptions amqpRetryOptions(
            @Value("${azure.servicebus.retry.max-retries:3}") int maxRetries,
            @Value("${azure.servicebus.retry.delay:1s}") Duration delay,
            @Value("${azure.servicebus.retry.max-delay:10s}") Duration maxDelay) {
        log.info("Configurando AmqpRetryOptions: maxRetries={}, delay={}, maxDelay={}, mode=EXPONENTIAL",
                maxRetries, delay, maxDelay);
        return new AmqpRetryOptions()
                .setMaxRetries(maxRetries)
                .setDelay(delay)
                .setMaxDelay(maxDelay)
                .setMode(AmqpRetryMode.EXPONENTIAL);
    }

    @Bean(name = "outputSenderClient")
    @Primary
    public ServiceBusSenderClient serviceBusOutputSenderClient(AmqpRetryOptions retryOptions) {
        log.info("Creando ServiceBusSenderClient con reintentos para cola de salida: {}", outputQueueName);
        outputSenderClient = new ServiceBusClientBuilder()
                .connectionString(outputConnectionString)
                .retryOptions(retryOptions)
                .sender()
                .queueName(outputQueueName)
                .buildClient();
        return outputSenderClient;
    }

    @Bean(name = "inputSenderClient")
    public ServiceBusSenderClient serviceBusInputSenderClient(AmqpRetryOptions retryOptions) {
        log.info("Creando ServiceBusSenderClient con reintentos para cola de entrada: {}", inputQueueName);
        inputSenderClient = new ServiceBusClientBuilder()
                .connectionString(inputSenderConnectionString)
                .retryOptions(retryOptions)
                .sender()
                .queueName(inputQueueName)
                .buildClient();
        return inputSenderClient;
    }

    @PreDestroy
    public void destroy() {
        log.info("Iniciando cierre controlado de senders de Service Bus (timeout 10s)...");
        closeWithTimeout("outputSenderClient", outputSenderClient);
        closeWithTimeout("inputSenderClient", inputSenderClient);
    }

    private void closeWithTimeout(String name, ServiceBusSenderClient client) {
        if (client != null) {
            try {
                CompletableFuture.runAsync(client::close)
                        .orTimeout(10, TimeUnit.SECONDS)
                        .join();
                log.info("{} cerrado exitosamente", name);
            } catch (Exception e) {
                log.warn("Cierre de {} excedió timeout acotado de 10s o falló: {}", name, e.getMessage());
            }
        }
    }
}
