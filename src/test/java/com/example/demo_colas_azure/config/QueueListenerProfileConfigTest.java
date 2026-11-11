package com.example.demo_colas_azure.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.ConversionService;

import com.azure.core.amqp.AmqpRetryOptions;
import com.example.demo_colas_azure.service.MessageProcessorService;
import com.example.demo_colas_azure.service.MultipleQueueListenerManager;
import com.example.demo_colas_azure.service.QueueListenerManager;
import com.example.demo_colas_azure.service.SingleQueueListenerManager;

class QueueListenerProfileConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(
                    StubDependenciesConfig.class,
                    SingleQueueListenerManager.class,
                    MultipleQueueListenerConfig.class)
            .withPropertyValues(
                    "azure.servicebus.input.connection-string=Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==",
                    "azure.servicebus.input.queue-name=input-queue");

    @Test
    void singleProfileActivatesSingleQueueListenerManagerOnly() {
        contextRunner
                .withPropertyValues(
                        "spring.profiles.active=single",
                        "azure.servicebus.input.drain-timeout=8s",
                        "azure.servicebus.input.close-timeout=4s")
                .run(context -> {
                    assertThat(context).hasSingleBean(QueueListenerManager.class);
                    assertThat(context).hasSingleBean(SingleQueueListenerManager.class);
                    assertThat(context).doesNotHaveBean(MultipleQueueListenerConfig.class);
                    assertThat(context).doesNotHaveBean(MultipleQueueListenerManager.class);

                    SingleQueueListenerManager manager = context.getBean(SingleQueueListenerManager.class);
                    assertThat(manager.getMode()).isEqualTo("single");
                    assertThat(manager.getDrainTimeout()).isEqualTo(Duration.ofSeconds(8));
                    assertThat(manager.getCloseTimeout()).isEqualTo(Duration.ofSeconds(4));
                });
    }

    @Test
    void defaultProfileFallsBackToSingleQueueListenerManager() {
        contextRunner
                .withPropertyValues("spring.profiles.default=single")
                .run(context -> {
                    assertThat(context).hasSingleBean(QueueListenerManager.class);
                    assertThat(context).hasSingleBean(SingleQueueListenerManager.class);
                    assertThat(context).doesNotHaveBean(MultipleQueueListenerConfig.class);
                });
    }

    @Test
    void multipleProfileActivatesMultipleQueueListenerConfigAndBeanWithDefaults() {
        contextRunner
                .withPropertyValues("spring.profiles.active=multiple")
                .run(context -> {
                    assertThat(context).hasSingleBean(QueueListenerManager.class);
                    assertThat(context).hasSingleBean(MultipleQueueListenerConfig.class);
                    assertThat(context).hasSingleBean(MultipleQueueListenerManager.class);
                    assertThat(context).doesNotHaveBean(SingleQueueListenerManager.class);

                    MultipleQueueListenerManager manager = context.getBean(MultipleQueueListenerManager.class);
                    assertThat(manager.getMode()).isEqualTo("multiple");
                    assertThat(manager.getMaxConcurrentCalls()).isEqualTo(10);
                    assertThat(manager.getPrefetchCount()).isEqualTo(20);
                });
    }

    @Test
    void multipleProfileRespectsCustomConcurrencyAndPrefetchProperties() {
        contextRunner
                .withPropertyValues(
                        "spring.profiles.active=multiple",
                        "azure.servicebus.input.multiple.max-concurrent-calls=25",
                        "azure.servicebus.input.multiple.prefetch-count=50",
                        "azure.servicebus.input.drain-timeout=12s",
                        "azure.servicebus.input.close-timeout=6s")
                .run(context -> {
                    MultipleQueueListenerManager manager = context.getBean(MultipleQueueListenerManager.class);
                    assertThat(manager.getMaxConcurrentCalls()).isEqualTo(25);
                    assertThat(manager.getPrefetchCount()).isEqualTo(50);
                    assertThat(manager.getDrainTimeout()).isEqualTo(Duration.ofSeconds(12));
                    assertThat(manager.getCloseTimeout()).isEqualTo(Duration.ofSeconds(6));
                });
    }

    @Configuration
    static class StubDependenciesConfig {

        @Bean
        MessageProcessorService messageProcessorService() {
            return mock(MessageProcessorService.class);
        }

        @Bean
        AmqpRetryOptions amqpRetryOptions() {
            return new AmqpRetryOptions();
        }

        @Bean
        ConversionService conversionService() {
            return ApplicationConversionService.getSharedInstance();
        }
    }
}
