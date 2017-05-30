/*
 * Copyright 2017-present Open Networking Laboratory
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
package org.onosproject.store.primitives.impl;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import com.google.common.base.Throwables;
import io.atomix.catalyst.transport.TransportException;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.session.CopycatSession;
import io.atomix.copycat.error.QueryException;
import io.atomix.copycat.error.UnknownClientException;
import io.atomix.copycat.error.UnknownSessionException;
import io.atomix.copycat.session.ClosedSessionException;
import org.onlab.util.Tools;
import org.onosproject.store.service.StorageException;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Retrying Copycat session.
 */
public class RetryingCopycatSession extends DelegatingCopycatSession {
    private final int maxRetries;
    private final long delayBetweenRetriesMillis;
    private final Logger log = getLogger(getClass());

    private final Predicate<Throwable> retryableCheck = e -> e instanceof ConnectException
            || e instanceof TimeoutException
            || e instanceof TransportException
            || e instanceof ClosedChannelException
            || e instanceof QueryException
            || e instanceof UnknownClientException
            || e instanceof UnknownSessionException
            || e instanceof ClosedSessionException
            || e instanceof StorageException.Unavailable;

    public RetryingCopycatSession(CopycatSession delegate, int maxRetries, long delayBetweenRetriesMillis) {
        super(delegate);
        this.maxRetries = maxRetries;
        this.delayBetweenRetriesMillis = delayBetweenRetriesMillis;
    }

    @Override
    public <T> CompletableFuture<T> submit(Query<T> query) {
        if (state() == CopycatSession.State.CLOSED) {
            return Tools.exceptionalFuture(new StorageException.Unavailable());
        }
        CompletableFuture<T> future = new CompletableFuture<>();
        submit(query, 1, future);
        return future;
    }

    private <T> void submit(Query<T> query, int attemptIndex, CompletableFuture<T> future) {
        delegate.submit(query).whenComplete((r, e) -> {
            if (e != null) {
                if (attemptIndex < maxRetries + 1 && retryableCheck.test(Throwables.getRootCause(e))) {
                    log.debug("Retry attempt ({} of {}). Failure due to {}",
                            attemptIndex, maxRetries, Throwables.getRootCause(e).getClass());
                    delegate.context().schedule(Duration.ofMillis(delayBetweenRetriesMillis),
                            () -> submit(query, attemptIndex + 1, future));
                } else {
                    future.completeExceptionally(e);
                }
            } else {
                future.complete(r);
            }
        });
    }
}
