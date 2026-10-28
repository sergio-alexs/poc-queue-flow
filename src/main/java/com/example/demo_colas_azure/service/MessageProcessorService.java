package com.example.demo_colas_azure.service;

import java.time.Instant;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;

import lombok.extern.slf4j.Slf4j;

/**
 * Servicio que procesa el mensaje recibido de la cola de entrada,
 * lo enriquece con metadatos y lo reenvía a la cola de salida.
 * Gestiona el límite de reintentos de entrega enviando a Dead-Letter Queue (DLQ)
 * si se supera el umbral configurado.
 */
@Service
@Slf4j
public class MessageProcessorService {

    public enum ProcessingResult {
        SUCCESS,
        ABANDONED,
        DEAD_LETTERED
    }

    private final ServiceBusSenderClient senderClient;
    private final long maxDeliveryCount;

    public MessageProcessorService(
            @Qualifier("outputSenderClient") ServiceBusSenderClient senderClient,
            @Value("${azure.servicebus.input.max-delivery-count:5}") long maxDeliveryCount) {
        this.senderClient = senderClient;
        this.maxDeliveryCount = maxDeliveryCount;
    }

    /**
     * Procesa un mensaje: lo enriquece con metadatos y lo envía a la cola de salida.
     * Si ocurre un error:
     * - Si deliveryCount >= maxDeliveryCount -> envía a Dead-Letter Queue (DLQ).
     * - De lo contrario -> ejecuta abandon() para reintento inmediato por Azure Service Bus.
     */
    public ProcessingResult processAndForward(ServiceBusReceivedMessageContext context) {
        try {
            String body = context.getMessage().getBody().toString();
            String messageId = context.getMessage().getMessageId();
            long deliveryCount = context.getMessage().getDeliveryCount();

            log.info("Procesando mensaje [id={}, deliveryCount={}/{}]: {}", 
                    messageId, deliveryCount, maxDeliveryCount, body);

            // Crear mensaje enriquecido con metadatos
            ServiceBusMessage enrichedMessage = new ServiceBusMessage(body);

            // Copiar propiedades originales del mensaje
            enrichedMessage.getApplicationProperties()
                    .putAll(context.getMessage().getApplicationProperties());

            // Agregar metadatos de enriquecimiento
            enrichedMessage.getApplicationProperties()
                    .put("processed_at", Instant.now().toString());
            enrichedMessage.getApplicationProperties()
                    .put("processing_status", "FORWARDED");

            // Enviar a la cola de salida
            senderClient.sendMessage(enrichedMessage);
            log.info("Mensaje [id={}] enviado a cola de salida exitosamente", messageId);

            // Completar el mensaje en la cola de entrada (se elimina de la cola)
            context.complete();
            log.info("Mensaje [id={}] completado en cola de entrada", messageId);

            return ProcessingResult.SUCCESS;

        } catch (Exception e) {
            long deliveryCount = context.getMessage().getDeliveryCount();
            log.error("Error procesando mensaje [id={}, deliveryCount={}/{}]: {}",
                    context.getMessage().getMessageId(), deliveryCount, maxDeliveryCount, e.getMessage(), e);

            if (deliveryCount >= maxDeliveryCount) {
                log.warn("Mensaje [id={}] alcanzó límite de reintentos ({}/{}). Moviendo a Dead-Letter Queue (DLQ)...",
                        context.getMessage().getMessageId(), deliveryCount, maxDeliveryCount);
                DeadLetterOptions deadLetterOptions = new DeadLetterOptions()
                        .setDeadLetterReason("MaxDeliveryAttemptsExceeded")
                        .setDeadLetterErrorDescription(e.getMessage() != null ? e.getMessage() : "Fallo en procesamiento");
                context.deadLetter(deadLetterOptions);
                return ProcessingResult.DEAD_LETTERED;
            } else {
                log.info("Abandonando mensaje [id={}] para reintento (intento {} de {})...",
                        context.getMessage().getMessageId(), deliveryCount, maxDeliveryCount);
                context.abandon();
                return ProcessingResult.ABANDONED;
            }
        }
    }
}
