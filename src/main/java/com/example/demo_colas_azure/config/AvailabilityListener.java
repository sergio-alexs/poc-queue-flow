package com.example.demo_colas_azure.config;

import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Registra en logs los cambios de estado de Liveness y Readiness de Kubernetes
 * para trazabilidad en monitoreo y prevención de Pods zombies.
 */
@Component
@Slf4j
public class AvailabilityListener {

    @EventListener
    public void onLivenessChange(AvailabilityChangeEvent<LivenessState> event) {
        log.info("[Kubernetes Probes] Cambio en LivenessState: {}", event.getState());
        if (event.getState() == LivenessState.BROKEN) {
            log.error("[Kubernetes Probes] Liveness roto: Kubernetes reiniciará este Pod");
        }
    }

    @EventListener
    public void onReadinessChange(AvailabilityChangeEvent<ReadinessState> event) {
        log.info("[Kubernetes Probes] Cambio en ReadinessState: {}", event.getState());
    }
}
