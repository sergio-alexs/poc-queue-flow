package com.example.demo_colas_azure.controller;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Endpoint para enviar mensajes de prueba a la cola de entrada de Azure Service Bus.
 * Facilita las pruebas de concepto sin necesidad de entrar al portal de Azure.
 */
@RestController
@RequestMapping("/api/messages")
@Slf4j
public class MessageController {

    private final ServiceBusSenderClient inputSenderClient;

    public MessageController(@Qualifier("inputSenderClient") ServiceBusSenderClient inputSenderClient) {
        this.inputSenderClient = inputSenderClient;
    }

    /**
     * POST /api/messages/send
     * Envía un mensaje a la cola de entrada configurada.
     * 
     * Body:
     * {
     *   "message": "Contenido del mensaje (texto o JSON)",
     *   "properties": { "customHeader": "valor" }
     * }
     */
    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> sendMessage(@RequestBody Map<String, Object> request) {
        Object messageObj = request.get("message");
        if (messageObj == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "El campo 'message' es obligatorio"
            ));
        }

        String messageContent = messageObj.toString();
        ServiceBusMessage message = new ServiceBusMessage(messageContent);
        String messageId = UUID.randomUUID().toString();
        message.setMessageId(messageId);

        // Copiar propiedades opcionales si vienen en el body
        Object propertiesObj = request.get("properties");
        if (propertiesObj instanceof Map<?, ?> customProps) {
            customProps.forEach((k, v) -> {
                if (k != null && v != null) {
                    message.getApplicationProperties().put(k.toString(), v);
                }
            });
        }

        log.info("Enviando mensaje de prueba [id={}] a la cola de entrada", messageId);
        inputSenderClient.sendMessage(message);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "SENT_TO_INPUT_QUEUE");
        response.put("messageId", messageId);
        response.put("content", messageContent);
        response.put("properties", message.getApplicationProperties());

        return ResponseEntity.ok(response);
    }
}
