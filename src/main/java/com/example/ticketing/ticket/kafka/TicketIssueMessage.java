package com.example.ticketing.ticket.kafka;

public record TicketIssueMessage (
        Long ticketId,
        Long userId
){
}
