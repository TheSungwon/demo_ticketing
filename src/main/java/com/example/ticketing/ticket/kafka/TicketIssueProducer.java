package com.example.ticketing.ticket.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TicketIssueProducer {

    private final KafkaTemplate<String, TicketIssueMessage> kafkaTemplate;

    public void send(Long ticketId, Long userId) {
        kafkaTemplate.send(
                TicketKafkaConfig.TICKET_ISSUE_TOPIC,
                userId.toString(),
                new TicketIssueMessage(ticketId, userId)
        );
    }
}