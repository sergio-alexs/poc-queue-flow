package com.example.demo_colas_azure.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusErrorSource;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusFailureReason;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;

@ExtendWith(MockitoExtension.class)
class QueueListenerManagerBehaviorTest {

    @Mock
    private MessageProcessorService messageProcessorService;

    @Test
    void multipleListenerValidatesConcurrencyAndShutdownParameters() {
        AmqpRetryOptions retryOptions = new AmqpRetryOptions();
        String cs = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==";

        assertThatThrownBy(() -> new MultipleQueueListenerManager(
                cs, "input-queue", Duration.ofMinutes(5), 0, 10, messageProcessorService, retryOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConcurrentCalls");

        assertThatThrownBy(() -> new MultipleQueueListenerManager(
                cs, "input-queue", Duration.ofMinutes(5), 5, -1, messageProcessorService, retryOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prefetchCount");

        assertThatThrownBy(() -> new MultipleQueueListenerManager(
                cs, "input-queue", Duration.ofMinutes(5), Duration.ofSeconds(-1), Duration.ofSeconds(5),
                5, 10, messageProcessorService, retryOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("drainTimeout");

        assertThatThrownBy(() -> new MultipleQueueListenerManager(
                cs, "input-queue", Duration.ofMinutes(5), Duration.ofSeconds(5), Duration.ZERO,
                5, 10, messageProcessorService, retryOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closeTimeout");
    }

    @Test
    void lifecycleStartAndStopWorksInSingleMode() {
        ServiceBusProcessorClient mockProcessor = mock(ServiceBusProcessorClient.class);
        TestableSingleQueueListenerManager manager = new TestableSingleQueueListenerManager(
                Duration.ofSeconds(2), Duration.ofSeconds(2), messageProcessorService, mockProcessor);

        assertThat(manager.isRunning()).isFalse();
        assertThat(manager.getMode()).isEqualTo("single");

        manager.startListening();
        assertThat(manager.isRunning()).isTrue();

        manager.stopListening();
        assertThat(manager.isRunning()).isFalse();

        InOrder order = inOrder(mockProcessor);
        order.verify(mockProcessor).start();
        order.verify(mockProcessor).stop();
        order.verify(mockProcessor).close();
    }

    @Test
    void lifecycleStartAndStopWorksInMultipleModeAndIsIdempotent() {
        ServiceBusProcessorClient mockProcessor = mock(ServiceBusProcessorClient.class);
        TestableMultipleQueueListenerManager manager = new TestableMultipleQueueListenerManager(
                Duration.ofSeconds(2), Duration.ofSeconds(2), 10, 20, messageProcessorService, mockProcessor);

        assertThat(manager.isRunning()).isFalse();
        assertThat(manager.getMode()).isEqualTo("multiple");

        manager.startListening();
        assertThat(manager.isRunning()).isTrue();
        verify(mockProcessor, times(1)).start();

        // Idempotente si ya está corriendo
        manager.startListening();
        verify(mockProcessor, times(1)).start();

        manager.stopListening();
        assertThat(manager.isRunning()).isFalse();

        InOrder order = inOrder(mockProcessor);
        order.verify(mockProcessor).stop();
        order.verify(mockProcessor).close();
    }

    @Test
    void multipleListenerDrainsInFlightMessagesBeforeClosingProcessor() throws Exception {
        ServiceBusProcessorClient mockProcessor = mock(ServiceBusProcessorClient.class);
        TestableMultipleQueueListenerManager manager = new TestableMultipleQueueListenerManager(
                Duration.ofSeconds(5), Duration.ofSeconds(2), 10, 20, messageProcessorService, mockProcessor);
        manager.startListening();

        int inFlightCount = 4;
        CountDownLatch enteredProcessing = new CountDownLatch(inFlightCount);
        CountDownLatch releaseProcessing = new CountDownLatch(1);

        when(messageProcessorService.processAndForward(any())).thenAnswer(invocation -> {
            enteredProcessing.countDown();
            releaseProcessing.await(5, TimeUnit.SECONDS);
            return MessageProcessorService.ProcessingResult.SUCCESS;
        });

        ExecutorService executor = Executors.newFixedThreadPool(inFlightCount + 1);
        try {
            for (int i = 0; i < inFlightCount; i++) {
                ServiceBusReceivedMessageContext ctx = mock(ServiceBusReceivedMessageContext.class);
                executor.submit(() -> manager.invokeHandleMessage(ctx));
            }

            assertThat(enteredProcessing.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(manager.getInFlightMessages()).isEqualTo(inFlightCount);

            Future<?> stopFuture = executor.submit(manager::stopListening);

            // Da tiempo a stopListening() a marcar running=false, llamar stop() y esperar en drain
            Thread.sleep(150);
            assertThat(manager.isRunning()).isFalse();
            verify(mockProcessor, times(1)).stop();
            verify(mockProcessor, never()).close();

            // Un mensaje nuevo que llegue tras running=false debe abandonarse inmediatamente
            ServiceBusReceivedMessage newMessage = mock(ServiceBusReceivedMessage.class);
            when(newMessage.getMessageId()).thenReturn("buffered-after-stop");
            ServiceBusReceivedMessageContext bufferedCtx = mock(ServiceBusReceivedMessageContext.class);
            when(bufferedCtx.getMessage()).thenReturn(newMessage);
            manager.invokeHandleMessage(bufferedCtx);
            verify(bufferedCtx).abandon();

            // Liberar los mensajes in-flight para completar el drenaje
            releaseProcessing.countDown();
            stopFuture.get(5, TimeUnit.SECONDS);

            verify(mockProcessor, times(1)).close();
            assertThat(manager.getInFlightMessages()).isZero();
            assertThat(manager.getMessagesProcessed()).isEqualTo(inFlightCount);
            assertThat(manager.getMessagesAbandoned()).isEqualTo(1L);
            assertThat(manager.getLastMessageProcessedAt()).isNotNull();
        } finally {
            releaseProcessing.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void stopListeningForcesCloseWhenDrainTimeoutExpires() throws Exception {
        ServiceBusProcessorClient mockProcessor = mock(ServiceBusProcessorClient.class);
        TestableSingleQueueListenerManager manager = new TestableSingleQueueListenerManager(
                Duration.ofMillis(120), Duration.ofSeconds(2), messageProcessorService, mockProcessor);
        manager.startListening();

        CountDownLatch enteredProcessing = new CountDownLatch(1);
        CountDownLatch releaseProcessing = new CountDownLatch(1);

        when(messageProcessorService.processAndForward(any())).thenAnswer(invocation -> {
            enteredProcessing.countDown();
            releaseProcessing.await(5, TimeUnit.SECONDS);
            return MessageProcessorService.ProcessingResult.SUCCESS;
        });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            ServiceBusReceivedMessageContext ctx = mock(ServiceBusReceivedMessageContext.class);
            executor.submit(() -> manager.invokeHandleMessage(ctx));
            assertThat(enteredProcessing.await(5, TimeUnit.SECONDS)).isTrue();

            manager.stopListening();

            assertThat(manager.isRunning()).isFalse();
            verify(mockProcessor, times(1)).stop();
            verify(mockProcessor, times(1)).close();
        } finally {
            releaseProcessing.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void multipleListenerProcessesConcurrentMessagesSafely() throws Exception {
        ServiceBusProcessorClient mockProcessor = mock(ServiceBusProcessorClient.class);
        TestableMultipleQueueListenerManager manager = new TestableMultipleQueueListenerManager(
                Duration.ofSeconds(5), Duration.ofSeconds(2), 10, 20, messageProcessorService, mockProcessor);
        manager.startListening();

        when(messageProcessorService.processAndForward(any()))
                .thenReturn(MessageProcessorService.ProcessingResult.SUCCESS);

        int totalMessages = 25;
        ExecutorService executor = Executors.newFixedThreadPool(totalMessages);
        CountDownLatch readyLatch = new CountDownLatch(totalMessages);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalMessages);

        try {
            for (int i = 0; i < totalMessages; i++) {
                executor.submit(() -> {
                    ServiceBusReceivedMessageContext ctx = mock(ServiceBusReceivedMessageContext.class);
                    readyLatch.countDown();
                    try {
                        startLatch.await(5, TimeUnit.SECONDS);
                        manager.invokeHandleMessage(ctx);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            assertThat(readyLatch.await(5, TimeUnit.SECONDS)).isTrue();
            startLatch.countDown();
            assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
            manager.stopListening();
        }

        assertThat(manager.getMessagesProcessed()).isEqualTo(totalMessages);
        assertThat(manager.getInFlightMessages()).isZero();
        assertThat(manager.getMessagesAbandoned()).isZero();
        assertThat(manager.getMessagesDeadLettered()).isZero();
        assertThat(manager.getMessagesErrored()).isZero();
        assertThat(manager.getLastMessageProcessedAt()).isNotNull();
    }

    @Test
    void singleListenerAbandonsInFlightMessageWhenStopped() {
        ServiceBusProcessorClient mockProcessor = mock(ServiceBusProcessorClient.class);
        TestableSingleQueueListenerManager manager = new TestableSingleQueueListenerManager(
                Duration.ofSeconds(2), Duration.ofSeconds(2), messageProcessorService, mockProcessor);

        ServiceBusReceivedMessage message = mock(ServiceBusReceivedMessage.class);
        when(message.getMessageId()).thenReturn("msg-1");
        ServiceBusReceivedMessageContext context = mock(ServiceBusReceivedMessageContext.class);
        when(context.getMessage()).thenReturn(message);

        manager.invokeHandleMessage(context);

        verify(context).abandon();
        assertThat(manager.getMessagesAbandoned()).isEqualTo(1L);
        assertThat(manager.getMessagesProcessed()).isZero();
        assertThat(manager.getInFlightMessages()).isZero();
        assertThat(manager.getLastMessageProcessedAt()).isNull();
    }

    @Test
    void handleErrorMarksFatalWhenNonTransientUnauthorizedErrorOccurs() {
        ServiceBusProcessorClient mockProcessor = mock(ServiceBusProcessorClient.class);
        TestableMultipleQueueListenerManager manager = new TestableMultipleQueueListenerManager(
                Duration.ofSeconds(2), Duration.ofSeconds(2), 10, 20, messageProcessorService, mockProcessor);

        ServiceBusException fatalEx = mock(ServiceBusException.class);
        when(fatalEx.isTransient()).thenReturn(false);
        when(fatalEx.getReason()).thenReturn(ServiceBusFailureReason.UNAUTHORIZED);
        when(fatalEx.getMessage()).thenReturn("Auth failed");
        ServiceBusErrorContext errorContext = mock(ServiceBusErrorContext.class);
        when(errorContext.getException()).thenReturn(fatalEx);
        when(errorContext.getErrorSource()).thenReturn(ServiceBusErrorSource.RECEIVE);

        manager.invokeHandleError(errorContext);

        assertThat(manager.isFatalErrorEncountered()).isTrue();
        assertThat(manager.getLastFatalErrorMessage()).contains("UNAUTHORIZED");
        assertThat(manager.getMessagesErrored()).isEqualTo(1L);
    }

    private static final class TestableMultipleQueueListenerManager extends MultipleQueueListenerManager {
        private final ServiceBusProcessorClient mockProcessor;

        TestableMultipleQueueListenerManager(
                Duration drainTimeout,
                Duration closeTimeout,
                int maxConcurrentCalls,
                int prefetchCount,
                MessageProcessorService messageProcessorService,
                ServiceBusProcessorClient mockProcessor) {
            super(
                    "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==",
                    "input-queue",
                    Duration.ofMinutes(5),
                    drainTimeout,
                    closeTimeout,
                    maxConcurrentCalls,
                    prefetchCount,
                    messageProcessorService,
                    new AmqpRetryOptions());
            this.mockProcessor = mockProcessor;
        }

        @Override
        protected ServiceBusProcessorClient buildProcessorClient() {
            return mockProcessor;
        }

        void invokeHandleMessage(ServiceBusReceivedMessageContext context) {
            handleMessage(context);
        }

        void invokeHandleError(ServiceBusErrorContext context) {
            handleError(context);
        }
    }

    private static final class TestableSingleQueueListenerManager extends SingleQueueListenerManager {
        private final ServiceBusProcessorClient mockProcessor;

        TestableSingleQueueListenerManager(
                Duration drainTimeout,
                Duration closeTimeout,
                MessageProcessorService messageProcessorService,
                ServiceBusProcessorClient mockProcessor) {
            super(
                    "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==",
                    "input-queue",
                    Duration.ofMinutes(5),
                    drainTimeout,
                    closeTimeout,
                    messageProcessorService,
                    new AmqpRetryOptions());
            this.mockProcessor = mockProcessor;
        }

        @Override
        protected ServiceBusProcessorClient buildProcessorClient() {
            return mockProcessor;
        }

        void invokeHandleMessage(ServiceBusReceivedMessageContext context) {
            handleMessage(context);
        }
    }
}
