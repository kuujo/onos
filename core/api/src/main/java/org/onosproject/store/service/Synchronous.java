/*
 * Copyright 2016-present Open Networking Foundation
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
package org.onosproject.store.service;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * DistributedPrimitive that is a synchronous (blocking) version of
 * another.
 *
 * @param <T> type of DistributedPrimitive
 */
public abstract class Synchronous<T extends DistributedPrimitive> implements DistributedPrimitive {

    private final T primitive;

    public Synchronous(T primitive) {
        this.primitive = primitive;
    }

    @Override
    public String name() {
        return primitive.name();
    }

    @Override
    public Type primitiveType() {
        return primitive.primitiveType();
    }

    @Override
    public CompletableFuture<Void> destroy() {
        return primitive.destroy();
    }

    protected <T> T complete(CompletableFuture<T> future, long operationTimeoutMillis) {
        return complete(
                future,
                operationTimeoutMillis,
                e -> new StorageException.Interrupted(),
                e -> new StorageException.Timeout(),
                e -> {
                    if (e.getCause() instanceof StorageException) {
                        throw (StorageException) e.getCause();
                    } else {
                        throw new StorageException(e.getCause());
                    }
                });
    }

    protected <T> T complete(
            CompletableFuture<T> future,
            long operationTimeoutMillis,
            Function<Throwable, ? extends RuntimeException> interruptedSupplier,
            Function<Throwable, ? extends RuntimeException> timeoutSupplier,
            Function<Throwable, ? extends RuntimeException> exceptionSupplier) {
        if (operationTimeoutMillis > 0) {
            try {
                return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw interruptedSupplier.apply(e);
            } catch (TimeoutException e) {
                throw timeoutSupplier.apply(e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof StorageException) {
                    throw (StorageException) e.getCause();
                }
                throw exceptionSupplier.apply(e.getCause());
            }
        } else {
            try {
                return future.join();
            } catch (CancellationException e) {
                throw interruptedSupplier.apply(e);
            } catch (CompletionException e) {
                if (e.getCause() instanceof StorageException) {
                    throw (StorageException) e.getCause();
                }
                throw exceptionSupplier.apply(e.getCause());
            }
        }
    }
}
