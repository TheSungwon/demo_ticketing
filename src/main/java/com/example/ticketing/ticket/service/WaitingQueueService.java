package com.example.ticketing.ticket.service;

import com.example.ticketing.ticket.kafka.TicketIssueProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class WaitingQueueService {
    private static final String PROCESSING_TIME_KEY = "ticket:processing:last";
    private static final String PROCESSING_RECENT_KEY = "ticket:processing:recent";

    private static final String SUCCESS_COUNT_KEY = "ticket:stats:success";
    private static final String FAILURE_COUNT_KEY = "ticket:stats:failure";

    private static final String REQUEST_COUNT_KEY = "ticket:stats:request";

    private static final String QUEUE_KEY = "ticket:waiting";
    private static final String QUEUE_USERS_KEY = "ticket:waiting:users";

    private final TicketIssueProducer ticketIssueProducer;
    private final StringRedisTemplate redisTemplate;
    private final TicketService ticketService;

    public Long enter(Long ticketId, Long userId) {
        Long added = redisTemplate.opsForSet()
                .add(QUEUE_USERS_KEY, userId.toString());

        // 이미 대기열에 있는 사용자
        if (added == null || added == 0L) {
            return getPosition(userId);
        }

        // 실제 요청 인원 증가
        redisTemplate.opsForValue()
                .increment(REQUEST_COUNT_KEY);

        // 처음 들어온 사용자만 실제 대기열에 추가
        return redisTemplate.opsForList()
                .rightPush(QUEUE_KEY, userId.toString());

    }

    public Long getWaitingCount() {
        Long size = redisTemplate.opsForList().size(QUEUE_KEY);
        return size != null ? size : 0L;
    }

    public Long getPosition(Long userId) {
        List<String> users = redisTemplate.opsForList().range(QUEUE_KEY, 0, -1);
        log.info("redistemplate users:{}", users);

        if(users == null) {
            return -1L;
        }

        int index = users.indexOf(userId.toString());
        return index == -1 ? -1L : index + 1L;
    }

    public void processNext(Long ticketId) {
        String userId = redisTemplate.opsForList().leftPop(QUEUE_KEY);
        log.info("redistemplate users:{}", userId);

        if(userId != null) {
            redisTemplate.opsForSet().remove(QUEUE_USERS_KEY, userId);

            ticketIssueProducer.send(ticketId, Long.valueOf(userId));
        }
    }

    public Long getEstimatedWaitingTime(Long userId) {

        Long position = getPosition(userId);

        if (position <= 0) {
            return 0L;
        }

        List<String> recent = redisTemplate.opsForList()
                .range(PROCESSING_RECENT_KEY, 0, -1);

        if(recent == null || recent.isEmpty()) {
            return position * 2;
        }

        double avgMs = recent.stream()
                .mapToLong(Long::parseLong).average().orElse(1000.0);
        long avgSec = Math.max(1, (long) Math.ceil(avgMs / 1000.0));

        return position * avgSec;
    }

    public Map<String, Long> getStats() {
        String request = redisTemplate.opsForValue().get(REQUEST_COUNT_KEY);

        String success = redisTemplate.opsForValue().get(SUCCESS_COUNT_KEY);

        String failure = redisTemplate.opsForValue().get(FAILURE_COUNT_KEY);

        return Map.of(
                "requestCount", request == null ? 0L : Long.parseLong(request),
                "waitingCount", getWaitingCount(),
                "successCount", success == null ? 0L : Long.parseLong(success),
                "failureCount", failure == null ? 0L : Long.parseLong(failure)
        );
    }

    public void resetStats(Long ticketId) {

        // DB 티켓 수량 초기화
        ticketService.reset(ticketId);

        // Redis 대기열 초기화
        redisTemplate.delete(QUEUE_KEY);
        redisTemplate.delete(QUEUE_USERS_KEY);

        // 테스트 통계 초기화
        redisTemplate.delete(REQUEST_COUNT_KEY);
        redisTemplate.delete(SUCCESS_COUNT_KEY);
        redisTemplate.delete(FAILURE_COUNT_KEY);

        // 처리시간 데이터 초기화
        redisTemplate.delete(PROCESSING_TIME_KEY);
        redisTemplate.delete(PROCESSING_RECENT_KEY);

        log.info("티켓 테스트 초기화 완료 : ticketId={}", ticketId);
    }

    public void enterTestUsers(Long ticketId, int count) {

        for (long userId = 1; userId <= count; userId++) {
            enter(ticketId, userId);
        }

        log.info(
                "테스트 사용자 등록 완료 : ticketId={}, count={}",
                ticketId,
                count
        );
    }
}
