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

import java.util.List;

import com.google.common.annotations.Beta;

/**
 * Resource transaction context.
 */
@Beta
public interface ResourceTransactionContext extends ResourceStoreBase, AutoCloseable {

    /**
     * Registers the resources in transactional way.
     * Resource registration must be done before resource allocation. The state after completion
     * of this method is all the resources are registered, or none of the given resources is registered.
     * The whole registration fails when any one of the resource can't be registered.
     *
     * @param resources resources to be registered
     */
    void register(List<? extends Resource> resources);

    /**
     * Unregisters the resources in transactional way.
     * The state after completion of this method is all the resources are unregistered,
     * or none of the given resources is unregistered. The whole unregistration fails when any one of the
     * resource can't be unregistered.
     *
     * @param ids resources to be unregistered
     */
    void unregister(List<? extends ResourceId> ids);

    /**
     * Allocates the specified resources to the specified consumer in transactional way.
     * The state after completion of this method is all the resources are allocated to the consumer,
     * or no resource is allocated to the consumer. The whole allocation fails when any one of
     * the resource can't be allocated.
     *
     * @param resources resources to be allocated
     * @param consumer resource consumer which the resources are allocated to
     */
    void allocate(List<? extends Resource> resources, ResourceConsumer consumer);

    /**
     * Releases the specified allocated resources in transactional way.
     * The state after completion of this method is all the resources
     * are released from the consumer, or no resource is released. The whole release fails
     * when any one of the resource can't be released. The size of the list of resources and
     * that of consumers must be equal. The resource and consumer with the same position from
     * the head of each list correspond to each other.
     *
     * @param allocations allocaitons to be released
     */
    void release(List<ResourceAllocation> allocations);

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
     */
    @Override
    void close();

}
