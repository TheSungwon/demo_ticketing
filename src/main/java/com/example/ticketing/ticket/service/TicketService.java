package com.example.ticketing.ticket.service;


import com.example.ticketing.ticket.entity.Ticket;
import com.example.ticketing.ticket.repository.TicketRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
@Slf4j
public class TicketService {

    private final TicketRepository ticketRepository;

    public Ticket getTicket(Long ticketId) {
        log.info("@@@@@ticketId: {}", ticketId);
        return ticketRepository.findById(ticketId)
                .orElseThrow(() -> new IllegalArgumentException("티켓을 찾을 수 없습니다"));
    }

    @Transactional
    public boolean issue(Long ticketId) {
        Ticket ticket = getTicket(ticketId);
        return ticket.issue();
    }

    @Transactional
    public void reset(Long ticketId) {
        Ticket ticket = getTicket(ticketId);
        ticket.resetQuantity();
    }
}
