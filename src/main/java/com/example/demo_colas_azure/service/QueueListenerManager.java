package com.example.demo_colas_azure.service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusFailureReason;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

/**
 * Gestiona el ciclo de vida del ServiceBusProcessorClient.
 * Permite iniciar y detener dinámicamente la escucha de mensajes
 * desde la cola de entrada de Azure Service Bus.
 * 
 * Blindado contra Pods zombies en Kubernetes:
 * - Cierre acotado con timeout de 10s para no bloquear el Graceful Shutdown del Pod.
 * - Detección de fallos fatales para alimentar los Liveness Probes de Kubernetes.
 * - Auto-renovación de bloqueo de mensajes (maxAutoLockRenewDuration).
 */
@Service
@Slf4j
public class QueueListenerManager {

    private final String inputConnectionString;
    private final String inputQueueName;
    private final MessageProcessorService messageProcessorService;
    private final AmqpRetryOptions amqpRetryOptions;
    private final Duration maxAutoLockRenewDuration;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ServiceBusProcessorClient processorClient;

    // Métricas atómicas
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong messagesAbandoned = new AtomicLong(0);
    private final AtomicLong messagesDeadLettered = new AtomicLong(0);
    private final AtomicLong messagesErrored = new AtomicLong(0);
    private volatile Instant lastMessageProcessedAt;

    // Detección de errores fatales para Liveness Probe de Kubernetes
    private final AtomicBoolean fatalErrorEncountered = new AtomicBoolean(false);
    private volatile String lastFatalErrorMessage;

    public QueueListenerManager(
            @Value("${azure.servicebus.input.connection-string}") String inputConnectionString,
            @Value("${azure.servicebus.input.queue-name}") String inputQueueName,
            @Value("${azure.servicebus.input.max-auto-lock-renew-duration:5m}") Duration maxAutoLockRenewDuration,
            MessageProcessorService messageProcessorService,
            AmqpRetryOptions amqpRetryOptions) {
        this.inputConnectionString = inputConnectionString;
        this.inputQueueName = inputQueueName;
        this.maxAutoLockRenewDuration = maxAutoLockRenewDuration;
        this.messageProcessorService = messageProcessorService;
        this.amqpRetryOptions = amqpRetryOptions;
    }

    /**
     * Inicia la escucha de mensajes creando un nuevo ServiceBusProcessorClient.
     * Si ya está corriendo, no hace nada.
     */
    public synchronized void startListening() {
        if (running.get()) {
            log.debug("El listener ya está corriendo, ignorando startListening()");
            return;
        }

        log.info("Iniciando listener para cola: {} (lockRenewal={}, retries={})", 
                inputQueueName, maxAutoLockRenewDuration, amqpRetryOptions.getMaxRetries());

        processorClient = new ServiceBusClientBuilder()
                .connectionString(inputConnectionString)
                .retryOptions(amqpRetryOptions)
                .processor()
                .queueName(inputQueueName)
                .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                .maxConcurrentCalls(1)
                .maxAutoLockRenewDuration(maxAutoLockRenewDuration)
                .processMessage(this::handleMessage)
                .processError(this::handleError)
                .buildProcessorClient();

        processorClient.start();
        running.set(true);

        log.info("Listener iniciado exitosamente para cola: {} (maxConcurrentCalls=1, maxAutoLockRenewDuration={})", 
                inputQueueName, maxAutoLockRenewDuration);
    }

    /**
     * Detiene la escucha de mensajes con un timeout de seguridad (10s)
     * para garantizar que el Pod termine ordenadamente sin quedar en estado Terminating / zombie.
     */
    public synchronized void stopListening() {
        if (!running.get()) {
            log.debug("El listener ya está detenido, ignorando stopListening()");
            return;
        }

        log.info("Deteniendo listener para cola: {}...", inputQueueName);

        // Marcar como detenido ANTES de cerrar para que mensajes in-flight sean abandonados
        running.set(false);

        if (processorClient != null) {
            ServiceBusProcessorClient clientToClose = processorClient;
            processorClient = null;

            try {
                // Cierre asíncrono acotado con timeout de 10 segundos
                CompletableFuture.runAsync(clientToClose::close)
                        .orTimeout(10, TimeUnit.SECONDS)
                        .join();
                log.info("Listener detenido exitosamente");
            } catch (Exception e) {
                log.warn("Cierre de ServiceBusProcessorClient excedió timeout de 10s o falló: {}", e.getMessage());
            }
        }
    }

    /**
     * Indica si el listener está actualmente escuchando mensajes.
     */
    public boolean isRunning() {
        return running.get();
    }

    public boolean isFatalErrorEncountered() {
        return fatalErrorEncountered.get();
    }

    public String getLastFatalErrorMessage() {
        return lastFatalErrorMessage;
    }

    // ---- Métodos de métricas ----

    public long getMessagesProcessed() {
        return messagesProcessed.get();
    }

    public long getMessagesAbandoned() {
        return messagesAbandoned.get();
    }

    public long getMessagesDeadLettered() {
        return messagesDeadLettered.get();
    }

    public long getMessagesErrored() {
        return messagesErrored.get();
    }

    public Instant getLastMessageProcessedAt() {
        return lastMessageProcessedAt;
    }

    /**
     * Handler de mensajes del processor.
     */
    private void handleMessage(ServiceBusReceivedMessageContext context) {
        if (!running.get()) {
            log.warn("Listener apagado - abandonando mensaje in-flight [id={}] para reintento",
                    context.getMessage().getMessageId());
            context.abandon();
            messagesAbandoned.incrementAndGet();
            return;
        }

        try {
            MessageProcessorService.ProcessingResult result = messageProcessorService.processAndForward(context);
            switch (result) {
                case SUCCESS -> {
                    messagesProcessed.incrementAndGet();
                    lastMessageProcessedAt = Instant.now();
                }
                case ABANDONED -> messagesAbandoned.incrementAndGet();
                case DEAD_LETTERED -> messagesDeadLettered.incrementAndGet();
            }
        } catch (Exception e) {
            messagesErrored.incrementAndGet();
            log.error("Error inesperado en handleMessage: {}", e.getMessage(), e);
        }
    }

    /**
     * Handler de errores del processor con detección de fallos fatales de conexión.
     */
    private void handleError(ServiceBusErrorContext context) {
        messagesErrored.incrementAndGet();
        Throwable ex = context.getException();
        log.error("Error en Service Bus processor - Fuente: {}, Razón: {}, Mensaje: {}",
                context.getErrorSource(),
                ex.getClass().getSimpleName(),
                ex.getMessage());

        if (ex instanceof ServiceBusException sbEx && !sbEx.isTransient()) {
            ServiceBusFailureReason reason = sbEx.getReason();
            if (reason == ServiceBusFailureReason.UNAUTHORIZED
                    || reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND) {
                log.error("FALLO FATAL NO RECUPERABLE en Service Bus: {}", reason);
                fatalErrorEncountered.set(true);
                lastFatalErrorMessage = "Error fatal Service Bus: " + reason + " - " + sbEx.getMessage();
            }
        }
    }

    /**
     * Limpieza acotada al destruir el bean (shutdown de la aplicación).
     */
    @PreDestroy
    public void destroy() {
        log.info("Destruyendo QueueListenerManager en fase de apagado...");
        stopListening();
    }
}
