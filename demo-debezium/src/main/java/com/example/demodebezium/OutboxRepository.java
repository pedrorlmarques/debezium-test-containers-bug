package com.example.demodebezium;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * This interface provides handles to database, to perform CRUD operations on the table `OUTBOX`.
 * The table is represented by the JPA entity {@link Outbox}.
 */
@Repository
public interface OutboxRepository extends JpaRepository<Outbox, Integer> {
}
