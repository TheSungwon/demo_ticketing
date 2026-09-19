# 🎟️ High-Concurrency Ticketing System

> **대규모 동시 요청을 가정한 티켓 예매 시스템**

Spring Boot · JPA · MariaDB · Redis · Kafka · Docker

[![Java](https://img.shields.io/badge/Java-17-orange?logo=openjdk)](https://www.oracle.com/java/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.1-6DB33F?logo=springboot)](https://spring.io/projects/spring-boot)
[![MariaDB](https://img.shields.io/badge/MariaDB-Database-003545?logo=mariadb)](https://mariadb.org/)
[![Redis](https://img.shields.io/badge/Redis-Queue%20%2F%20Cache-DC382D?logo=redis)](https://redis.io/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker)](https://www.docker.com/)

---

## 📌 Project Overview

100장의 한정된 티켓에 **1만 명의 사용자가 동시에 접근하는 상황**을 가정하여 구현한 티켓팅 시스템입니다.

단순한 CRUD 구현이 아니라 다음과 같은 실제 시스템 문제를 작은 규모로 재현하고 단계적으로 해결하는 것을 목표로 합니다.

```text
              10,000 Concurrent Requests
                         │
                         ▼
                ┌─────────────────┐
                │   Waiting Queue │
                │      Redis      │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │      Kafka      │
                │  Async Event    │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │    Consumer     │
                │  Ticket Issue   │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │    MariaDB      │
                │  Inventory      │
                └─────────────────┘
```

### 핵심 검증 목표

* 대량 요청을 Redis Queue로 제어
* 동일 사용자의 중복 Queue 입장 방지
* Kafka를 이용한 티켓 발급 비동기 처리
* 한정된 재고에 대한 동시성 문제 분석
* DB 수준의 원자적 재고 차감
* 10,000건 요청 부하 테스트
* TPS / 처리시간 / P95 / P99 측정
* Docker 기반 실행 환경 구성

---

# 📊 Project Dashboard

| Metric                 |     Target |
| ---------------------- | ---------: |
| 🎫 Total Tickets       |    **100** |
| 👥 Concurrent Requests | **10,000** |
| ✅ Expected Success     |    **100** |
| ❌ Expected Failure     |  **9,900** |
| 🚨 Overselling         |      **0** |
| ⚡ TPS                  |        TBD |
| ⏱️ P95                 |        TBD |
| ⏱️ P99                 |        TBD |

> TPS, P95, P99는 실제 부하 테스트 이후 측정값으로 업데이트합니다.

---

# 🧩 1. Problem

티켓 수량이 제한되어 있는데 짧은 시간 동안 많은 사용자가 동시에 요청하면 다음과 같은 문제가 발생합니다.

### Traffic Problem

```text
Normal Traffic

User ────────► API ────────► DB


Traffic Spike

User ─┐
User ─┤
User ─┤
User ─┤
User ─┼──────► API ────────► DB
User ─┤
User ─┤
User ─┤
...   │
10,000┘
```

모든 요청이 DB에 직접 접근하면 DB에 순간적으로 많은 부하가 발생할 수 있습니다.

### Concurrency Problem

예를 들어 티켓이 1장 남아 있는 상황에서 동시에 두 요청이 들어오면:

```text
Request A ──► remainingQuantity > 0 ──► true
                                      │
Request B ──► remainingQuantity > 0 ──► true
                                      │
                                      ▼
                              두 요청 모두 발급
```

단순한 `조회 → 감소` 구조에서는 **Lost Update / Overselling** 문제가 발생할 수 있습니다.

따라서 이 프로젝트에서는 다음 문제를 각각 분리하여 접근합니다.

```text
Traffic Control
      │
      ▼
Redis Queue

Request / Processing Separation
      │
      ▼
Kafka

Inventory Consistency
      │
      ▼
MariaDB Atomic Update
```

---

# 🏗️ 2. Architecture

## 전체 구조

```text
┌──────────────┐
│    Client    │
│  HTML / JS   │
└──────┬───────┘
       │ HTTP
       ▼
┌────────────────────┐
│    Spring Boot     │
│      REST API      │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│       Redis        │
│                    │
│  Waiting Queue     │
│  Duplicate Check   │
│  Statistics        │
└─────────┬──────────┘
          │
          │ Ticket Issue Event
          ▼
┌────────────────────┐
│       Kafka        │
│   ticket-issue     │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│   Kafka Consumer   │
│                    │
│ Ticket Issue       │
│ Processing         │
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│      MariaDB       │
│                    │
│ Ticket Inventory   │
│ Persistent Data    │
└────────────────────┘
```

---

# 🔄 3. Request Flow

사용자가 티켓 구매를 요청하면 다음과 같은 흐름으로 처리됩니다.

```text
User Request
     │
     ▼
Spring Boot
     │
     ▼
Redis Queue
     │
     ├── Already Waiting
     │       │
     │       └── Return Existing Position
     │
     └── New User
             │
             ▼
        Queue Processing
             │
             ▼
        Kafka Producer
             │
             ▼
        Kafka Topic
             │
             ▼
        Kafka Consumer
             │
             ▼
        Ticket Issue
             │
             ▼
          MariaDB
```

핵심은 **HTTP 요청 처리와 실제 티켓 발급 처리를 분리하는 것**입니다.

```text
Request Handling
       ≠
Ticket Issuing
```

---

# 🧠 4. Why Redis?

Redis는 티켓 자체의 영속적인 데이터를 저장하기 위한 목적이 아니라 **빠르게 변하는 대기열 상태를 관리하기 위한 용도**로 사용합니다.

## Waiting Queue

Redis List를 이용해 FIFO 방식의 대기열을 구성합니다.

```text
User 1
User 2
User 3
User 4
  │
  ▼
┌───────────────┐
│   FIFO Queue  │
└───────────────┘
```

주요 Redis 연산:

```text
RPUSH   → Queue 등록
LPOP    → 다음 사용자 처리
LRANGE  → Queue 위치 조회
LLEN    → 현재 대기 인원 조회
```

---

## Duplicate Prevention

Redis Set을 이용하여 동일 사용자의 중복 Queue 입장을 방지합니다.

```text
QUEUE_USERS

1001
1002
1003
1004
```

```java
Long added = redisTemplate.opsForSet()
        .add(QUEUE_USERS_KEY, userId.toString());

if (added == null || added == 0L) {
    return getPosition(userId);
}
```

`SADD`의 반환값을 이용하여 신규 등록 여부를 판단합니다.

```text
1 → 새롭게 추가됨
0 → 이미 존재
```

즉:

```text
                    Redis Set
                       │
              ┌────────┴────────┐
              │                 │
           New User        Existing User
              │                 │
              ▼                 ▼
         Queue 등록        기존 순번 반환
```

---

# 📨 5. Why Kafka?

Kafka는 단순히 "빠르게 처리하기 위해" 사용하는 것이 아니라 **티켓 발급 요청과 실제 발급 처리를 분리하기 위해** 사용합니다.

```text
Waiting Queue
      │
      ▼
  Producer
      │
      ▼
┌─────────────┐
│    Kafka    │
│ ticket-issue│
└──────┬──────┘
       │
       ▼
   Consumer
       │
       ▼
 Ticket Issue
```

## Producer

대기열에서 다음 사용자를 꺼내 티켓 발급 이벤트를 Kafka에 전달합니다.

```java
kafkaTemplate.send(
        TicketKafkaConfig.TICKET_ISSUE_TOPIC,
        userId.toString(),
        new TicketIssueMessage(
                ticketId,
                userId
        )
);
```

## Consumer

Kafka에서 메시지를 받아 실제 티켓 발급을 수행합니다.

```java
@KafkaListener(
        topics = TicketKafkaConfig.TICKET_ISSUE_TOPIC,
        groupId = "ticket-issuer"
)
public void consume(TicketIssueMessage message) {

    boolean issued =
            ticketService.issue(message.ticketId());

    if (issued) {
        // 성공 처리
    } else {
        // 실패 처리
    }
}
```

### Kafka의 역할

```text
HTTP Request
      │
      │ 빠르게 수신
      ▼
Queue
      │
      │ 이벤트 전달
      ▼
Kafka
      │
      │ 비동기 처리
      ▼
Consumer
      │
      ▼
Ticket Issue
```

> **Kafka가 DB 동시성 문제를 해결하는 것은 아닙니다.**

Kafka는 메시지 처리 구조를 분리하고, 재고 정합성은 별도의 DB 동시성 제어 문제로 접근합니다.

---

# 🎫 6. Ticket Inventory

티켓은 한정된 수량을 가집니다.

```text
Total Quantity      100
Remaining Quantity  100
```

기본적인 발급 로직은 다음과 같습니다.

```java
public boolean issue() {

    if (remainingQuantity <= 0) {
        return false;
    }

    remainingQuantity--;

    return true;
}
```

하지만 이 방식은 동시 요청 상황에서 안전하지 않을 수 있습니다.

```text
if (remainingQuantity > 0) {
    remainingQuantity--;
    return true;
}
```

### 문제

```text
Thread A                  Thread B
   │                         │
   ├─ Read = 1              │
   │                         ├─ Read = 1
   │                         │
   ├─ Decrease              │
   │                         ├─ Decrease
   │                         │
   ▼                         ▼
       Overselling
```

따라서 최종적으로 DB 수준에서 원자적인 재고 차감을 검토합니다.

```sql
UPDATE ticket
SET remaining_quantity = remaining_quantity - 1
WHERE id = ?
  AND remaining_quantity > 0;
```

그리고 영향을 받은 row 수를 확인합니다.

```text
affected rows = 1
        │
        ▼
    Issue Success


affected rows = 0
        │
        ▼
    Sold Out
```

---

# 🔐 7. Concurrency Control

이 프로젝트에서 가장 중요하게 검증하는 부분입니다.

## Test Scenario

```text
Tickets       = 100
Requests      = 10,000
```

목표:

```text
Success       = 100
Failure       = 9,900
Overselling   = 0
```

즉, 요청 수가 아무리 많아도 **티켓보다 많이 발급되지 않는 것**을 검증합니다.

```text
10,000 Requests
       │
       ▼
┌──────────────────┐
│   Concurrency    │
│      Control     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  100 Successful  │
│  9,900 Failed    │
│  0 Overselling   │
└──────────────────┘
```

---

# ⏳ 8. Waiting Queue

대기열에서는 다음 정보를 제공합니다.

* 현재 대기 인원
* 사용자의 대기 순번
* 예상 대기시간
* 중복 Queue 입장 여부

## Queue Position

Redis List에서 사용자의 위치를 조회합니다.

```java
List<String> users =
        redisTemplate.opsForList()
                .range(QUEUE_KEY, 0, -1);

int index =
        users.indexOf(userId.toString());

return index == -1
        ? -1L
        : index + 1L;
```

---

## Estimated Waiting Time

최근 티켓 처리시간을 Redis List에 저장합니다.

```text
Recent Processing Time

120ms
150ms
130ms
180ms
...
```

평균 처리시간과 현재 대기 순번을 이용해 예상 대기시간을 계산합니다.

```text
Estimated Wait Time
        =
Queue Position
        ×
Average Processing Time
```

현재 방식은 학습 및 시각화를 위한 단순 추정 방식이며, 향후 실제 처리량 기반 계산 방식으로 개선할 예정입니다.

---

# 📈 9. Monitoring

테스트 화면에서는 다음 데이터를 확인할 수 있도록 구성합니다.

```text
┌─────────────────────────────────────┐
│         TICKETING MONITOR           │
├─────────────────────────────────────┤
│                                     │
│  Total Tickets          100         │
│  Total Requests       10,000        │
│  Current Waiting        1,500       │
│                                     │
│  Success                 100        │
│  Failure               9,900        │
│                                     │
│  Avg Processing       ... ms        │
│  TPS                  ...          │
│  P95                  ... ms       │
│  P99                  ... ms       │
│                                     │
└─────────────────────────────────────┘
```

### 측정 대상

| Metric          | Description           |
| --------------- | --------------------- |
| Total Requests  | 전체 요청 수               |
| Success         | 티켓 발급 성공 수            |
| Failure         | 발급 실패 수               |
| Current Waiting | 현재 대기 인원              |
| Processing Time | 실제 티켓 발급 처리시간         |
| TPS             | 초당 처리량                |
| P95             | 상위 95% 요청의 latency 기준 |
| P99             | 상위 99% 요청의 latency 기준 |

---

# 🧪 10. Load Test

티켓 수량보다 훨씬 많은 사용자가 동시에 요청하는 상황을 테스트합니다.

| Scenario |  Users | Tickets |
| -------- | -----: | ------: |
| Test 1   |    100 |     100 |
| Test 2   |  1,000 |     100 |
| Test 3   | 10,000 |     100 |

### 검증 항목

```text
✓ Ticket Overselling
✓ Success Count
✓ Failure Count
✓ Queue Throughput
✓ Average Processing Time
✓ TPS
✓ Total Processing Time
✓ P95 Latency
✓ P99 Latency
```

### 목표

```text
┌───────────────────────────────┐
│       10,000 Requests         │
├───────────────────────────────┤
│                               │
│  Ticket Capacity      100     │
│                               │
│  Success              100     │
│  Failure            9,900     │
│  Overselling            0     │
│                               │
└───────────────────────────────┘
```

실제 성능 지표는 부하 테스트 완료 후 측정값으로 기록합니다.

---

# 🛠️ 11. Troubleshooting & Engineering Notes

## Redis Set 반환 타입

Redis Set의 `add()` 반환값을 확인하여 신규 Queue 등록 여부를 판단합니다.

```java
Long added = redisTemplate.opsForSet()
        .add(...);
```

```text
1 → 신규 등록
0 → 이미 존재
```

이를 통해 동일 사용자의 중복 Queue 입장을 방지합니다.

---

## Kafka Processing Time

Kafka Producer의 전송 시간을 측정하면 실제 티켓 발급 처리시간을 측정할 수 없습니다.

따라서 실제 발급이 수행되는 Consumer에서 측정합니다.

```java
long start = System.currentTimeMillis();

boolean issued =
        ticketService.issue(message.ticketId());

long processingTime =
        System.currentTimeMillis() - start;
```

측정 범위:

```text
Consumer
   │
   ├── Start Timer
   │
   ├── Ticket Issue
   │
   └── End Timer
```

이를 통해 실제 발급 처리시간을 Redis 통계에 기록하고 평균 처리시간과 예상 대기시간 계산에 활용합니다.

---

# 🧱 12. Technology Stack

| Category        | Technology              | Purpose                            |
| --------------- | ----------------------- | ---------------------------------- |
| Language        | Java 17                 | Backend                            |
| Framework       | Spring Boot             | REST API / Application             |
| Persistence     | Spring Data JPA         | ORM / Data Access                  |
| Database        | MariaDB                 | Ticket Inventory / Persistent Data |
| Queue           | Redis                   | Waiting Queue / Duplicate Check    |
| Statistics      | Redis                   | Processing Statistics              |
| Message Broker  | Apache Kafka            | Async Ticket Issue                 |
| Container       | Docker                  | Infrastructure                     |
| Build           | Maven                   | Dependency / Build                 |
| Frontend        | HTML / CSS / JavaScript | Test / Monitoring UI               |
| Server          | Linux                   | Deployment                         |
| Version Control | Git / GitHub            | Source Management                  |

---

# 📁 13. Project Structure

```text
ticketing/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com.example.ticketing/
│   │   │       ├── ticket/
│   │   │       │   ├── controller/
│   │   │       │   ├── service/
│   │   │       │   ├── entity/
│   │   │       │   └── repository/
│   │   │       │
│   │   │       └── kafka/
│   │   │           ├── producer/
│   │   │           ├── consumer/
│   │   │           └── config/
│   │   │
│   │   └── resources/
│   │       ├── application.yml
│   │       └── static/
│   │           └── index.html
│   │
│   └── test/
│
├── docker-compose.yml
├── pom.xml
└── README.md
```

---

# 🚀 14. Getting Started

## Requirements

```text
Java 17
Maven
Docker
MariaDB
```

Redis와 Kafka는 Docker를 통해 실행합니다.

### 1. Infrastructure 실행

```bash
docker compose up -d
```

컨테이너 상태 확인:

```bash
docker ps
```

### 2. Spring Boot 실행

macOS / Linux:

```bash
./mvnw spring-boot:run
```

Windows:

```bash
mvnw.cmd spring-boot:run
```

### 3. Test UI

Spring Boot 실행 후:

```text
http://localhost:8080
```

---

# 🔌 15. API

## Queue 입장

```http
POST /api/tickets/{ticketId}/queue?userId={userId}
```

## Queue 상태

```http
GET /api/tickets/{ticketId}/queue?userId={userId}
```

## 다음 사용자 처리

```http
POST /api/tickets/{ticketId}/queue/process
```

## 통계

```http
GET /api/tickets/{ticketId}/stats
```

## 통계 초기화

```http
POST /api/tickets/{ticketId}/stats/reset
```

## 테스트 사용자 생성

```http
POST /api/tickets/{ticketId}/queue/test?count=10000
```

---

# 📋 16. Implementation Status

### Completed

```text
✓ Spring Boot Backend
✓ REST API
✓ JPA / MariaDB
✓ Redis Queue
✓ Redis Duplicate Prevention
✓ Redis Statistics
✓ Kafka Producer
✓ Kafka Consumer
✓ Ticket Issue Processing
✓ Queue Position
✓ Estimated Waiting Time
✓ Test User Generation
✓ Test Reset
✓ Test Frontend
```

### In Progress

```text
○ DB Inventory Concurrency Control
○ TPS Measurement
○ Average Processing Time Measurement
○ P95 / P99 Measurement
○ 10,000 User Load Test
○ Kafka Failure / Retry Strategy
○ Redis Queue Atomicity Improvement
```

### Planned

```text
○ Docker-based Full Environment
○ Linux Server Deployment
○ Frontend Enhancement
○ Next.js Migration
○ System Monitoring
○ Failure Scenario Testing
```

---

# 💡 17. Engineering Insights

## 01. Queue와 Database는 서로 다른 역할을 가진다.

```text
Redis
│
└── 빠르게 변하는 대기 상태

MariaDB
│
└── 영속적인 티켓 재고
```

Redis는 대기열을 관리하고, MariaDB는 최종적인 티켓 데이터를 관리합니다.

---

## 02. Kafka와 DB 동시성 제어는 별개의 문제다.

```text
Kafka
│
└── 메시지 처리 구조 분리


DB Concurrency Control
│
└── 재고 정합성 보장
```

Kafka를 사용한다고 해서 DB의 Overselling 문제가 자동으로 해결되는 것은 아닙니다.

---

## 03. 기술보다 기술의 역할이 중요하다.

이 프로젝트에서는 단순히 Redis, Kafka, JPA를 사용하는 것이 목적이 아닙니다.

```text
Problem
   │
   ▼
Why?
   │
   ▼
Technology
   │
   ▼
Implementation
   │
   ▼
Measurement
```

각 기술이 어떤 문제를 해결하는지 기준으로 설계합니다.

---

## 04. 성능은 측정해야 한다.

시스템의 성능을 단순히 "빠르다"고 판단하지 않고 다음 지표를 기준으로 측정합니다.

```text
Requests
   │
   ├── TPS
   ├── Average Processing Time
   ├── P95
   └── P99
```

---

# 🗺️ 18. Roadmap

```text
                    CURRENT
                       │
                       ▼
              ┌─────────────────┐
              │   Redis Queue   │
              └────────┬────────┘
                       ▼
              ┌─────────────────┐
              │      Kafka      │
              └────────┬────────┘
                       ▼
              ┌─────────────────┐
              │  JPA / MariaDB  │
              └────────┬────────┘
                       │
                       ▼
              DB Concurrency Control
                       │
                       ▼
              10,000 User Load Test
                       │
                       ▼
                 TPS / P95 / P99
                       │
                       ▼
              Kafka Retry / Failure
                       │
                       ▼
              Redis Atomic Queue
                       │
                       ▼
                     Docker
                       │
                       ▼
                 Linux Server
                       │
                       ▼
                Next.js Frontend
```

---

# 🎯 19. Project Goal

이 프로젝트의 최종 목표는 단순한 티켓 CRUD 시스템을 만드는 것이 아닙니다.

```text
              Simple CRUD
                   │
                   ▼
             Waiting Queue
                   │
                   ▼
           Async Processing
                   │
                   ▼
        Inventory Consistency
                   │
                   ▼
        Concurrency Control
                   │
                   ▼
             Load Testing
                   │
                   ▼
        Performance Analysis
                   │
                   ▼
              Deployment
```

**대규모 트래픽에서 발생할 수 있는 문제를 직접 재현하고,**

**각 문제에 대한 해결 방법을 구현한 뒤,**

**부하 테스트와 성능 지표를 통해 실제로 검증하는 것**을 목표로 합니다.

---

## 📌 Key Takeaways

```text
Redis
→ Traffic / Queue Management

Kafka
→ Asynchronous Event Processing

MariaDB
→ Persistent Inventory

Atomic UPDATE
→ Inventory Consistency

Load Test
→ System Verification

TPS / P95 / P99
→ Performance Measurement
```

> **이 프로젝트는 "기술을 많이 사용한 프로젝트"가 아니라
> "트래픽과 동시성 문제를 단계적으로 해결하고 검증하는 프로젝트"를 목표로 합니다.**
