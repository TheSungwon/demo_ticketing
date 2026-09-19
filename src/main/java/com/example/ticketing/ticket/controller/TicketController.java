package com.example.ticketing.ticket.controller;

import com.example.ticketing.ticket.entity.Ticket;
import com.example.ticketing.ticket.service.TicketService;
import com.example.ticketing.ticket.service.WaitingQueueService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/tickets")
@Slf4j
public class TicketController {
    private final TicketService ticketService;
    private final WaitingQueueService waitingQueueService;

    @GetMapping("/{ticketId}")
    public Ticket getTicket(@PathVariable("ticketId") Long ticketId) {
        log.info("ticketId: {}", ticketId);
        return ticketService.getTicket(ticketId);
    }

    //→ 대기열 입장
    @PostMapping("/{ticketId}/queue")
    public Long enterQueue(@PathVariable Long ticketId, @RequestParam Long userId) {
        return waitingQueueService.enter(ticketId, userId);
    }

    //→ 대기 인원 + 내 순번
    @GetMapping("/{ticketId}/queue")
    public Map<String, Long> getQueueStatus(
            @PathVariable Long ticketId,
            @RequestParam Long userId
    ) {
        return Map.of(
                "waitingCount", waitingQueueService.getWaitingCount(),
                "position", waitingQueueService.getPosition(userId),
                "estimatedWaitingTime",
                waitingQueueService.getEstimatedWaitingTime(userId)
        );
    }

    @PostMapping("/{ticketId}/queue/process")
    public void processNext(@PathVariable Long ticketId) {
        waitingQueueService.processNext(ticketId);
    }

    @GetMapping("/{ticketId}/stats")
    public Map<String, Long> getStats(@PathVariable Long ticketId) {
        return waitingQueueService.getStats();
    }

    @PostMapping("/{ticketId}/stats/reset")
    public void resetStats(@PathVariable Long ticketId) {
        waitingQueueService.resetStats(ticketId);
    }

    @PostMapping("/{ticketId}/queue/test")
    public void enterTesteUsers(@PathVariable Long ticketId, @RequestParam int count) {
        waitingQueueService.enterTestUsers(ticketId, count);
        //POST /api/tickets/1/queue/test?count=10000
    }

}
