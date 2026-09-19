package com.example.ticketing.ticket.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class Ticket {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private int totalQuantity;

    @Column(nullable = false)
    private int remainingQuantity;

    public boolean issue() {
        if(remainingQuantity <= 0) {
            return false;
        }
        remainingQuantity -= 1;
        return true;
    }

    public void resetQuantity() {
        this.remainingQuantity = this.totalQuantity;
    }
}