/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.store.primitives2;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.onlab.util.Tools;
import org.onosproject.store.primitives.TransactionId;
import org.onosproject.store.service.CommitStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction coordinator.
 */
public class TransactionCoordinator {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final TransactionManager transactionManager;

    public TransactionCoordinator(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<CommitStatus> prepare(Transaction transaction) {
        long totalParticipants = transaction.participants().stream()
                .filter(p -> !p.transactionLog().isEmpty())
                .count();

        if (totalParticipants == 0) {
            log.debug("No transaction participants, skipping commit", totalParticipants);
            return CompletableFuture.completedFuture(CommitStatus.SUCCESS);
        } else if (totalParticipants == 1) {
            log.debug("Committing transaction {} via 1 participant", transaction.transactionId());
            TransactionalPrimitive participant = transaction.participants().stream()
                    .filter(p -> !p.transactionLog().isEmpty())
                    .findFirst()
                    .get();
            return participant.async()
                    .prepareAndCommit()
                    .thenApply(v -> (boolean) v ? CommitStatus.SUCCESS : CommitStatus.FAILURE);
        } else {
            log.debug("Committing transaction {} via {} participants", transaction.transactionId(), totalParticipants);
            Set<TransactionalPrimitive<?, ?>> activeParticipants = transaction.participants().stream()
                    .filter(p -> !p.transactionLog().isEmpty())
                    .collect(Collectors.toSet());

            CompletableFuture<CommitStatus> status = transactionManager.begin(transaction.transactionId())
                    .thenCompose(v -> prepare(transaction.transactionId(), activeParticipants))
                    .thenCompose(result -> result
                            ? transactionManager.commit(transaction.transactionId())
                            .thenCompose(v -> commit(transaction.transactionId(), activeParticipants))
                            .thenApply(v -> CommitStatus.SUCCESS)
                            : transactionManager.rollback(transaction.transactionId())
                            .thenCompose(v -> rollback(transaction.transactionId(), activeParticipants))
                            .thenApply(v -> CommitStatus.FAILURE));
            return status.thenCompose(v -> transactionManager.complete(transaction.transactionId()).thenApply(u -> v));
        }
    }

    /**
     * Performs the prepare phase of the two-phase commit protocol for the given transaction participants.
     *
     * @param transactionParticipants the transaction participants for which to prepare the transaction
     * @return a completable future indicating whether <em>all</em> prepares succeeded
     */
    protected CompletableFuture<Boolean> prepare(TransactionId transactionId, Set<TransactionalPrimitive<?, ?>> transactionParticipants) {
        log.trace("Preparing transaction {} via {}", transactionId, transactionParticipants);
        return Tools.allOf(transactionParticipants.stream()
                .map(p -> p.async().prepare())
                .collect(Collectors.toList()))
                .thenApply(list -> list.stream().reduce(Boolean::logicalAnd).orElse(true));
    }

    /**
     * Performs the commit phase of the two-phase commit protocol for the given transaction participants.
     *
     * @param transactionParticipants the transaction participants for which to commit the transaction
     * @return a completable future to be completed once the commits are complete
     */
    protected CompletableFuture<Void> commit(TransactionId transactionId, Set<TransactionalPrimitive<?, ?>> transactionParticipants) {
        log.trace("Committing transaction {} via {}", transactionId, transactionParticipants);
        return CompletableFuture.allOf(transactionParticipants.stream()
                .map(p -> p.async().commit())
                .toArray(CompletableFuture[]::new));
    }

    /**
     * Rolls back transactions for the given participants.
     *
     * @param transactionParticipants the transaction participants for which to roll back the transaction
     * @return a completable future to be completed once the rollbacks are complete
     */
    protected CompletableFuture<Void> rollback(TransactionId transactionId, Set<TransactionalPrimitive<?, ?>> transactionParticipants) {
        log.trace("Rolling back transaction {} via {}", transactionId, transactionParticipants);
        return CompletableFuture.allOf(transactionParticipants.stream()
                .map(p -> p.async().rollback())
                .toArray(CompletableFuture[]::new));
    }
}
