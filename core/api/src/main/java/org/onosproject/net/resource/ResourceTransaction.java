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
package org.onosproject.net.resource;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Resource transaction.
 */
public interface ResourceTransaction extends ResourceQueryService, AutoCloseable {

    /**
     * Allocates the specified resource to the specified user.
     *
     * @param consumer resource user which the resource is allocated to
     * @param resource resource to be allocated
     */
    default void allocate(ResourceConsumer consumer, Resource resource) {
        allocate(consumer, ImmutableList.of(checkNotNull(resource)));
    }

    /**
     * Transactionally allocates the specified resources to the specified user.
     * All allocations are made when this method succeeds, or no allocation is made when this method fails.
     *
     * @param consumer  resource user which the resources are allocated to
     * @param resources resources to be allocated
     */
    void allocate(ResourceConsumer consumer, List<? extends Resource> resources);

    /**
     * Transactionally allocates the specified resources to the specified user.
     * All allocations are made when this method succeeds, or no allocation is made when this method fails.
     *
     * @param consumer  resource user which the resources are allocated to
     * @param resources resources to be allocated
     */
    default void allocate(ResourceConsumer consumer, Resource... resources) {
        allocate(checkNotNull(consumer), Arrays.asList(checkNotNull(resources)));
    }

    /**
     * Releases the specified resource allocation.
     *
     * @param allocation resource allocation to be released
     */
    default void release(ResourceAllocation allocation) {
        release(ImmutableList.of(checkNotNull(allocation)));
    }

    /**
     * Transactionally releases the specified resource allocations.
     * All allocations are released when this method succeeded, or no allocation is released when this method fails.
     *
     * @param allocations resource allocations to be released
     */
    void release(List<ResourceAllocation> allocations);

    /**
     * Transactionally releases the specified resource allocations.
     * All allocations are released when this method succeeded, or no allocation is released when this method fails.
     *
     * @param allocations resource allocations to be released
     */
    default void release(ResourceAllocation... allocations) {
        release(ImmutableList.copyOf(checkNotNull(allocations)));
    }

    /**
     * Transactionally releases the resources allocated to the specified consumer.
     * All allocations are released when this method succeeded, or no allocation is released when this method fails.
     *
     * @param consumer consumer whose allocated resources are to be released
     */
    void release(ResourceConsumer consumer);

    /**
     * Commits the transaction.
     *
     * @return indicates whether the transaction was successfully committed
     */
    ResourceCommitStatus commit();

    /**
     * Aborts the transaction.
     */
    void abort();

    /**
     * Closes the transaction.
     * <p>
     * If the transaction has not been committed, it will be aborted.
     */
    @Override
    void close();

}
