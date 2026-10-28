package com.example.demo_colas_azure.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.example.demo_colas_azure.repository.AppConfigRepository;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Cron que revisa periódicamente la variable de control en la base de datos
 * y enciende o apaga el listener de la cola de Azure Service Bus.
 * 
 * Se ejecuta al iniciar la aplicación (@PostConstruct) y luego cada 1 minuto (@Scheduled).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ListenerScheduler {

    private static final String LISTENER_CONFIG_KEY = "queue_listener_enabled";

    private final AppConfigRepository appConfigRepository;
    private final QueueListenerManager queueListenerManager;

    /**
     * Sincronización inicial al levantar la aplicación.
     * No requiere esperar 1 minuto para comenzar a escuchar si está habilitado en BD.
     */
    @PostConstruct
    public void init() {
        log.info("[Scheduler] Ejecutando sincronización inicial de estado al iniciar aplicación...");
        checkListenerStatus();
    }

    /**
     * Ejecuta cada 60 segundos (1 minuto).
     * Consulta la BD para determinar si el listener debe estar encendido o apagado
     * y reconcilia el estado actual.
     */
    @Scheduled(fixedRate = 60000)
    public void checkListenerStatus() {
        try {
            boolean shouldBeEnabled = appConfigRepository.findByConfigKey(LISTENER_CONFIG_KEY)
                    .map(config -> "true".equalsIgnoreCase(config.getConfigValue()))
                    .orElse(false);

            boolean isCurrentlyRunning = queueListenerManager.isRunning();

            if (shouldBeEnabled && !isCurrentlyRunning) {
                log.info("[Scheduler] Variable BD='true', listener detenido -> INICIANDO listener");
                queueListenerManager.startListening();
            } else if (!shouldBeEnabled && isCurrentlyRunning) {
                log.info("[Scheduler] Variable BD='false', listener activo -> DETENIENDO listener");
                queueListenerManager.stopListening();
            } else {
                log.debug("[Scheduler] Sin cambios - BD='{}', listener='{}'",
                        shouldBeEnabled ? "encendido" : "apagado",
                        isCurrentlyRunning ? "corriendo" : "detenido");
            }

        } catch (Exception e) {
            log.error("[Scheduler] Error al verificar estado del listener: {}", e.getMessage(), e);
        }
    }
}
