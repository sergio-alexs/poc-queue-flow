package com.example.demo_colas_azure.service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusFailureReason;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;

import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementación base para gestionar el ciclo de vida del {@link ServiceBusProcessorClient}.
 * Centraliza el drenaje ordenado de mensajes en vuelo, cierre acotado con timeout,
 * detección de errores fatales para Kubernetes probes y métricas atómicas thread-safe.
 */
@Slf4j
public abstract class AbstractQueueListenerManager implements QueueListenerManager {

    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    protected final String inputConnectionString;
    protected final String inputQueueName;
    protected final MessageProcessorService messageProcessorService;
    protected final AmqpRetryOptions amqpRetryOptions;
    protected final Duration maxAutoLockRenewDuration;
    @Getter
    protected final Duration drainTimeout;
    @Getter
    protected final Duration closeTimeout;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile ServiceBusProcessorClient processorClient;

    // Control de mensajes en vuelo para drenaje ordenado al detener el listener
    private final AtomicInteger inFlightMessages = new AtomicInteger(0);
    private final Object inFlightLock = new Object();

    // Métricas atómicas thread-safe
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong messagesAbandoned = new AtomicLong(0);
    private final AtomicLong messagesDeadLettered = new AtomicLong(0);
    private final AtomicLong messagesErrored = new AtomicLong(0);
    private final AtomicReference<Instant> lastMessageProcessedAt = new AtomicReference<>();

    // Detección de errores fatales para Liveness/Readiness Probe de Kubernetes
    private final AtomicBoolean fatalErrorEncountered = new AtomicBoolean(false);
    private volatile String lastFatalErrorMessage;

    protected AbstractQueueListenerManager(
            String inputConnectionString,
            String inputQueueName,
            Duration maxAutoLockRenewDuration,
            MessageProcessorService messageProcessorService,
            AmqpRetryOptions amqpRetryOptions) {
        this(inputConnectionString, inputQueueName, maxAutoLockRenewDuration,
                DEFAULT_DRAIN_TIMEOUT, DEFAULT_CLOSE_TIMEOUT,
                messageProcessorService, amqpRetryOptions);
    }

    protected AbstractQueueListenerManager(
            String inputConnectionString,
            String inputQueueName,
            Duration maxAutoLockRenewDuration,
            Duration drainTimeout,
            Duration closeTimeout,
            MessageProcessorService messageProcessorService,
            AmqpRetryOptions amqpRetryOptions) {
        if (drainTimeout == null || drainTimeout.isNegative()) {
            throw new IllegalArgumentException("'drainTimeout' no puede ser nulo ni negativo");
        }
        if (closeTimeout == null || closeTimeout.isNegative() || closeTimeout.isZero()) {
            throw new IllegalArgumentException("'closeTimeout' debe ser mayor que cero");
        }
        this.inputConnectionString = inputConnectionString;
        this.inputQueueName = inputQueueName;
        this.maxAutoLockRenewDuration = maxAutoLockRenewDuration;
        this.drainTimeout = drainTimeout;
        this.closeTimeout = closeTimeout;
        this.messageProcessorService = messageProcessorService;
        this.amqpRetryOptions = amqpRetryOptions;
    }

    /**
     * Inicia la escucha de mensajes creando un nuevo {@link ServiceBusProcessorClient}.
     * Si ya está corriendo, no hace nada.
     */
    @Override
    public synchronized void startListening() {
        if (running.get()) {
            log.debug("El listener [modo={}] ya está corriendo, ignorando startListening()", getMode());
            return;
        }

        log.info("Iniciando listener [modo={}] para cola: {} ({})",
                getMode(), inputQueueName, getProcessorConfigurationSummary());

        processorClient = buildProcessorClient();
        processorClient.start();
        running.set(true);

        log.info("Listener [modo={}] iniciado exitosamente para cola: {} ({})",
                getMode(), inputQueueName, getProcessorConfigurationSummary());
    }

    /**
     * Detiene la escucha de mensajes de forma ordenada:
     * 1. Marca el listener como detenido para rechazar/abandonar nuevos mensajes en buffer.
     * 2. Pausa nuevas entregas en el {@link ServiceBusProcessorClient} ({@code stop()}).
     * 3. Espera hasta {@code drainTimeout} a que terminen los mensajes en vuelo.
     * 4. Cierra el cliente de forma acotada dentro de {@code closeTimeout}.
     */
    @Override
    public synchronized void stopListening() {
        if (!running.get()) {
            log.debug("El listener [modo={}] ya está detenido, ignorando stopListening()", getMode());
            return;
        }

        log.info("Deteniendo listener [modo={}] para cola: {} (inFlight={}, drainTimeout={}, closeTimeout={})...",
                getMode(), inputQueueName, inFlightMessages.get(), drainTimeout, closeTimeout);

        // Marcar como detenido ANTES de pausar/cerrar para abandonar mensajes pendientes en buffer
        running.set(false);

        if (processorClient != null) {
            ServiceBusProcessorClient clientToClose = processorClient;
            processorClient = null;

            try {
                clientToClose.stop();
            } catch (Exception e) {
                log.warn("Error al pausar ServiceBusProcessorClient [modo={}] antes de drenar: {}",
                        getMode(), e.getMessage());
            }

            waitForInFlightMessages();

            try {
                // Cierre asíncrono acotado con closeTimeout
                CompletableFuture.runAsync(clientToClose::close)
                        .orTimeout(closeTimeout.toMillis(), TimeUnit.MILLISECONDS)
                        .join();
                log.info("Listener [modo={}] detenido exitosamente", getMode());
            } catch (Exception e) {
                log.warn("Cierre de ServiceBusProcessorClient [modo={}] excedió timeout de {} o falló: {}",
                        getMode(), closeTimeout, e.getMessage());
            }
        }
    }

    private void waitForInFlightMessages() {
        if (drainTimeout.isZero()) {
            return;
        }

        long deadlineNanos = System.nanoTime() + drainTimeout.toNanos();
        synchronized (inFlightLock) {
            while (inFlightMessages.get() > 0) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    log.warn("Timeout de drenaje ({}) alcanzado en modo [{}] con {} mensaje(s) aún en vuelo; forzando cierre",
                            drainTimeout, getMode(), inFlightMessages.get());
                    break;
                }
                long millis = remainingNanos / 1_000_000L;
                int nanos = (int) (remainingNanos % 1_000_000L);
                try {
                    inFlightLock.wait(millis, nanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Espera de drenaje interrumpida en modo [{}] con {} mensaje(s) aún en vuelo",
                            getMode(), inFlightMessages.get());
                    break;
                }
            }
        }
    }

    protected ServiceBusProcessorClient buildProcessorClient() {
        ServiceBusClientBuilder.ServiceBusProcessorClientBuilder builder = new ServiceBusClientBuilder()
                .connectionString(inputConnectionString)
                .retryOptions(amqpRetryOptions)
                .processor()
                .queueName(inputQueueName)
                .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                .disableAutoComplete()
                .maxAutoLockRenewDuration(maxAutoLockRenewDuration)
                .processMessage(this::handleMessage)
                .processError(this::handleError);

        configureProcessor(builder);
        return builder.buildProcessorClient();
    }

    /**
     * Permite a cada implementación configurar la concurrencia y prefetch del processor.
     */
    protected abstract void configureProcessor(ServiceBusClientBuilder.ServiceBusProcessorClientBuilder builder);

    /**
     * Descripción legible de la configuración de concurrencia para trazabilidad en logs.
     */
    protected abstract String getProcessorConfigurationSummary();

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isFatalErrorEncountered() {
        return fatalErrorEncountered.get();
    }

    @Override
    public String getLastFatalErrorMessage() {
        return lastFatalErrorMessage;
    }

    // ---- Métodos de métricas ----

    @Override
    public long getMessagesProcessed() {
        return messagesProcessed.get();
    }

    @Override
    public long getMessagesAbandoned() {
        return messagesAbandoned.get();
    }

    @Override
    public long getMessagesDeadLettered() {
        return messagesDeadLettered.get();
    }

    @Override
    public long getMessagesErrored() {
        return messagesErrored.get();
    }

    @Override
    public Instant getLastMessageProcessedAt() {
        return lastMessageProcessedAt.get();
    }

    @Override
    public long getInFlightMessages() {
        return inFlightMessages.get();
    }

    /**
     * Handler de mensajes del processor.
     * No es synchronized para permitir ejecución concurrente cuando maxConcurrentCalls > 1.
     */
    protected void handleMessage(ServiceBusReceivedMessageContext context) {
        inFlightMessages.incrementAndGet();
        try {
            if (!running.get()) {
                log.warn("Listener apagado - abandonando mensaje entrante [id={}] para reintento",
                        context.getMessage().getMessageId());
                try {
                    context.abandon();
                    messagesAbandoned.incrementAndGet();
                } catch (Exception ex) {
                    log.warn("No se pudo abandonar mensaje entrante [id={}] durante el cierre: {}",
                            context.getMessage().getMessageId(), ex.getMessage());
                }
                return;
            }

            try {
                MessageProcessorService.ProcessingResult result = messageProcessorService.processAndForward(context);
                switch (result) {
                    case SUCCESS -> {
                        messagesProcessed.incrementAndGet();
                        Instant now = Instant.now();
                        lastMessageProcessedAt.accumulateAndGet(now,
                                (prev, next) -> prev == null || next.isAfter(prev) ? next : prev);
                    }
                    case ABANDONED -> messagesAbandoned.incrementAndGet();
                    case DEAD_LETTERED -> messagesDeadLettered.incrementAndGet();
                }
            } catch (Exception e) {
                messagesErrored.incrementAndGet();
                log.error("Error inesperado en handleMessage [modo={}]: {}", getMode(), e.getMessage(), e);
            }
        } finally {
            if (inFlightMessages.decrementAndGet() == 0) {
                synchronized (inFlightLock) {
                    inFlightLock.notifyAll();
                }
            }
        }
    }

    /**
     * Handler de errores del processor con detección de fallos fatales de conexión.
     */
    protected void handleError(ServiceBusErrorContext context) {
        messagesErrored.incrementAndGet();
        Throwable ex = context.getException();
        log.error("Error en Service Bus processor [modo={}] - Fuente: {}, Razón: {}, Mensaje: {}",
                getMode(),
                context.getErrorSource(),
                ex.getClass().getSimpleName(),
                ex.getMessage());

        if (ex instanceof ServiceBusException sbEx && !sbEx.isTransient()) {
            ServiceBusFailureReason reason = sbEx.getReason();
            if (reason == ServiceBusFailureReason.UNAUTHORIZED
                    || reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND) {
                log.error("FALLO FATAL NO RECUPERABLE en Service Bus [modo={}]: {}", getMode(), reason);
                lastFatalErrorMessage = "Error fatal Service Bus: " + reason + " - " + sbEx.getMessage();
                fatalErrorEncountered.set(true);
            }
        }
    }

    /**
     * Limpieza acotada al destruir el bean (shutdown de la aplicación).
     */
    @PreDestroy
    public void destroy() {
        log.info("Destruyendo QueueListenerManager [modo={}] en fase de apagado...", getMode());
        stopListening();
    }
}
