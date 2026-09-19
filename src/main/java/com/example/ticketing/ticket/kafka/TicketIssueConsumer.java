package com.example.ticketing.ticket.kafka;

import com.example.ticketing.ticket.service.TicketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
@Slf4j
public class TicketIssueConsumer {

    private static final String PROCESSING_TIME_KEY = "ticket:processing:last";
    private static final String PROCESSING_RECENT_KEY = "ticket:processing:recent";

    private static final String SUCCESS_COUNT_KEY = "ticket:stats:success";
    private static final String FAILURE_COUNT_KEY = "ticket:stats:failure";

    private final TicketService ticketService;
    private final StringRedisTemplate redisTemplate;

    @KafkaListener(
            topics = TicketKafkaConfig.TICKET_ISSUE_TOPIC,
            groupId = "ticket-issuer"
    )
    public void consume(TicketIssueMessage message) {
        long start = System.currentTimeMillis();

        boolean issued = ticketService.issue(message.ticketId());

        if(issued) {
            redisTemplate.opsForValue().increment(SUCCESS_COUNT_KEY);

            long processingTime = System.currentTimeMillis() - start;
            redisTemplate.opsForValue().set(
                    PROCESSING_TIME_KEY,
                    String.valueOf(processingTime)
            );
            redisTemplate.opsForList().leftPush(PROCESSING_RECENT_KEY, String.valueOf(processingTime));
            redisTemplate.opsForList().trim(PROCESSING_RECENT_KEY, 0, 19);

            log.info("발급 성공 : ticketId={}, userId={}, processingTime={}", message.ticketId(), message.userId(), processingTime);
        }else{
            redisTemplate.opsForValue().increment(FAILURE_COUNT_KEY);
            log.info("발급 실패 : ticketId={}, userId={}", message.ticketId(), message.userId());
        }
    }
}
