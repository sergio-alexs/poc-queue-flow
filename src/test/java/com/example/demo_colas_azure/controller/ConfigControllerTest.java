package com.example.demo_colas_azure.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.example.demo_colas_azure.entity.AppConfig;
import com.example.demo_colas_azure.repository.AppConfigRepository;
import com.example.demo_colas_azure.service.QueueListenerManager;

@ExtendWith(MockitoExtension.class)
class ConfigControllerTest {

    @Mock
    private AppConfigRepository appConfigRepository;

    @Mock
    private QueueListenerManager queueListenerManager;

    @InjectMocks
    private ConfigController configController;

    @Test
    void getListenerStatusReturnsModeAndInFlightMetrics() {
        AppConfig config = new AppConfig(1L, "queue_listener_enabled", "true");
        when(appConfigRepository.findByConfigKey("queue_listener_enabled")).thenReturn(Optional.of(config));
        when(queueListenerManager.isRunning()).thenReturn(true);
        when(queueListenerManager.getMode()).thenReturn("multiple");
        when(queueListenerManager.getInFlightMessages()).thenReturn(3L);
        when(queueListenerManager.getMessagesProcessed()).thenReturn(15L);

        ResponseEntity<Map<String, Object>> response = configController.getListenerStatus();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .containsEntry("enabled", true)
                .containsEntry("running", true)
                .containsEntry("mode", "multiple");

        @SuppressWarnings("unchecked")
        Map<String, Object> metrics = (Map<String, Object>) response.getBody().get("metrics");
        assertThat(metrics)
                .containsEntry("inFlightMessages", 3L)
                .containsEntry("messagesProcessed", 15L);
    }

    @Test
    void updateListenerStatusFalseStopsListenerAndPersistsInDb() {
        AppConfig existing = new AppConfig(1L, "queue_listener_enabled", "true");
        when(appConfigRepository.findByConfigKey("queue_listener_enabled")).thenReturn(Optional.of(existing));
        when(appConfigRepository.save(any(AppConfig.class))).thenAnswer(inv -> inv.getArgument(0));
        when(queueListenerManager.isRunning()).thenReturn(false);
        when(queueListenerManager.getMode()).thenReturn("multiple");

        ResponseEntity<Map<String, Object>> response = configController.updateListenerStatus(Map.of("enabled", false));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(existing.getConfigValue()).isEqualTo("false");
        verify(appConfigRepository).save(existing);
        verify(queueListenerManager).stopListening();
        assertThat(response.getBody())
                .containsEntry("enabled", false)
                .containsEntry("running", false)
                .containsEntry("mode", "multiple");
    }

    @Test
    void updateListenerStatusTrueStartsListenerAndPersistsInDb() {
        when(appConfigRepository.findByConfigKey("queue_listener_enabled")).thenReturn(Optional.empty());
        when(appConfigRepository.save(any(AppConfig.class))).thenAnswer(inv -> inv.getArgument(0));
        when(queueListenerManager.isRunning()).thenReturn(true);
        when(queueListenerManager.getMode()).thenReturn("single");

        ResponseEntity<Map<String, Object>> response = configController.updateListenerStatus(Map.of("enabled", true));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        verify(appConfigRepository).save(any(AppConfig.class));
        verify(queueListenerManager).startListening();
        assertThat(response.getBody())
                .containsEntry("enabled", true)
                .containsEntry("running", true)
                .containsEntry("mode", "single");
    }
}

