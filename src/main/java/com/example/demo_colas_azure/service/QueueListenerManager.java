package com.example.demo_colas_azure.service;

import java.time.Instant;

/**
 * Contrato para gestionar el ciclo de vida del listener de Azure Service Bus
 * (activar/detener dinámicamente la escucha de mensajes y exponer métricas).
 */
public interface QueueListenerManager {

    /**
     * Inicia la escucha de mensajes desde la cola de entrada.
     * Si ya está corriendo, no hace nada.
     */
    void startListening();

    /**
     * Detiene la escucha de mensajes de forma acotada.
     * Si ya está detenido, no hace nada.
     */
    void stopListening();

    /**
     * Indica si el listener está actualmente escuchando mensajes.
     */
    boolean isRunning();

    boolean isFatalErrorEncountered();

    String getLastFatalErrorMessage();

    // ---- Métodos de métricas ----

    long getMessagesProcessed();

    long getMessagesAbandoned();

    long getMessagesDeadLettered();

    long getMessagesErrored();

    Instant getLastMessageProcessedAt();

    default long getInFlightMessages() {
        return 0L;
    }

    /**
     * Indica el modo de procesamiento activo (ej. {@code single} o {@code multiple}).
     */
    default String getMode() {
        return "single";
    }
}
