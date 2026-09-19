package com.example.ticketing.ticket.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic ticketIssueTopic() {
        return new NewTopic(
                TicketKafkaConfig.TICKET_ISSUE_TOPIC,
                1,
                (short) 1
        );
    }
}
