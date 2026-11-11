package com.example.demo_colas_azure.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import com.example.demo_colas_azure.service.QueueListenerManager;

@ExtendWith(MockitoExtension.class)
class ServiceBusHealthIndicatorTest {

    @Mock
    private QueueListenerManager queueListenerManager;

    @InjectMocks
    private ServiceBusHealthIndicator healthIndicator;

    @Test
    void healthShouldReturnUpWithoutLastMessageProcessedAtWhenNull() {
        when(queueListenerManager.isFatalErrorEncountered()).thenReturn(false);
        when(queueListenerManager.isRunning()).thenReturn(true);
        when(queueListenerManager.getMessagesProcessed()).thenReturn(0L);
        when(queueListenerManager.getMessagesAbandoned()).thenReturn(0L);
        when(queueListenerManager.getMessagesDeadLettered()).thenReturn(0L);
        when(queueListenerManager.getMessagesErrored()).thenReturn(0L);
        when(queueListenerManager.getLastMessageProcessedAt()).thenReturn(null);

        Health health = healthIndicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsEntry("listenerRunning", true)
                .containsEntry("inFlightMessages", 0L)
                .containsEntry("messagesProcessed", 0L)
                .containsEntry("messagesAbandoned", 0L)
                .containsEntry("messagesDeadLettered", 0L)
                .containsEntry("messagesErrored", 0L)
                .doesNotContainKey("lastMessageProcessedAt");
    }

    @Test
    void healthShouldIncludeLastMessageProcessedAtWhenPresent() {
        Instant processedAt = Instant.parse("2026-11-10T19:00:00Z");
        when(queueListenerManager.isFatalErrorEncountered()).thenReturn(false);
        when(queueListenerManager.isRunning()).thenReturn(true);
        when(queueListenerManager.getMode()).thenReturn("multiple");
        when(queueListenerManager.getInFlightMessages()).thenReturn(4L);
        when(queueListenerManager.getMessagesProcessed()).thenReturn(3L);
        when(queueListenerManager.getMessagesAbandoned()).thenReturn(1L);
        when(queueListenerManager.getMessagesDeadLettered()).thenReturn(0L);
        when(queueListenerManager.getMessagesErrored()).thenReturn(0L);
        when(queueListenerManager.getLastMessageProcessedAt()).thenReturn(processedAt);

        Health health = healthIndicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsEntry("mode", "multiple")
                .containsEntry("inFlightMessages", 4L)
                .containsEntry("messagesProcessed", 3L)
                .containsEntry("lastMessageProcessedAt", processedAt);
    }

    @Test
    void healthShouldReturnDownWithErrorWhenFatalErrorEncountered() {
        when(queueListenerManager.isFatalErrorEncountered()).thenReturn(true);
        when(queueListenerManager.getLastFatalErrorMessage()).thenReturn("Error fatal Service Bus: UNAUTHORIZED");

        Health health = healthIndicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails())
                .containsEntry("reason", "Fallo fatal en conexión o autenticación de Azure Service Bus")
                .containsEntry("error", "Error fatal Service Bus: UNAUTHORIZED")
                .containsEntry("listenerRunning", false);
    }

    @Test
    void healthShouldReturnDownWithoutErrorDetailWhenFatalErrorMessageIsNull() {
        when(queueListenerManager.isFatalErrorEncountered()).thenReturn(true);
        when(queueListenerManager.getLastFatalErrorMessage()).thenReturn(null);

        Health health = healthIndicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails())
                .containsEntry("reason", "Fallo fatal en conexión o autenticación de Azure Service Bus")
                .containsEntry("listenerRunning", false)
                .doesNotContainKey("error");
    }
}

