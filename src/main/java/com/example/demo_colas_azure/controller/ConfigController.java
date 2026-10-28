package com.example.demo_colas_azure.controller;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo_colas_azure.entity.AppConfig;
import com.example.demo_colas_azure.repository.AppConfigRepository;
import com.example.demo_colas_azure.service.QueueListenerManager;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Endpoints REST para consultar y cambiar el estado del listener.
 */
@RestController
@RequestMapping("/api/config")
@RequiredArgsConstructor
@Slf4j
public class ConfigController {

    private static final String LISTENER_CONFIG_KEY = "queue_listener_enabled";

    private final AppConfigRepository appConfigRepository;
    private final QueueListenerManager queueListenerManager;

    /**
     * GET /api/config/listener-status
     * Retorna el estado actual del listener y métricas de procesamiento.
     */
    @GetMapping("/listener-status")
    public ResponseEntity<Map<String, Object>> getListenerStatus() {
        boolean dbEnabled = appConfigRepository.findByConfigKey(LISTENER_CONFIG_KEY)
                .map(config -> "true".equalsIgnoreCase(config.getConfigValue()))
                .orElse(false);

        boolean isRunning = queueListenerManager.isRunning();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("enabled", dbEnabled);
        response.put("running", isRunning);

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("messagesProcessed", queueListenerManager.getMessagesProcessed());
        metrics.put("messagesAbandoned", queueListenerManager.getMessagesAbandoned());
        metrics.put("messagesDeadLettered", queueListenerManager.getMessagesDeadLettered());
        metrics.put("messagesErrored", queueListenerManager.getMessagesErrored());
        metrics.put("lastMessageProcessedAt", queueListenerManager.getLastMessageProcessedAt());
        response.put("metrics", metrics);

        return ResponseEntity.ok(response);
    }

    /**
     * PUT /api/config/listener-status
     * Cambia el estado del listener en la BD y aplica el cambio de forma inmediata en memoria.
     *
     * Body: { "enabled": true } o { "enabled": false }
     */
    @PutMapping("/listener-status")
    public ResponseEntity<Map<String, Object>> updateListenerStatus(
            @RequestBody Map<String, Boolean> request) {

        Boolean enabled = request.get("enabled");
        if (enabled == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "El campo 'enabled' es requerido (true/false)"
            ));
        }

        AppConfig config = appConfigRepository.findByConfigKey(LISTENER_CONFIG_KEY)
                .orElseGet(() -> {
                    AppConfig newConfig = new AppConfig();
                    newConfig.setConfigKey(LISTENER_CONFIG_KEY);
                    return newConfig;
                });

        config.setConfigValue(String.valueOf(enabled));
        appConfigRepository.save(config);

        log.info("Estado del listener guardado en BD: {}", enabled);

        // Aplicación inmediata del cambio
        if (enabled) {
            queueListenerManager.startListening();
        } else {
            queueListenerManager.stopListening();
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("enabled", enabled);
        response.put("running", queueListenerManager.isRunning());
        response.put("message", enabled 
                ? "Listener encendido inmediatamente y guardado en BD." 
                : "Listener apagado inmediatamente y guardado en BD. Mensajes in-flight abandonados para reintento.");

        return ResponseEntity.ok(response);
    }
}
