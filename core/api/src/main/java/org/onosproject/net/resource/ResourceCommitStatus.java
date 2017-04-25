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

/**
 * Resource transaction commit status.
 */
public enum ResourceCommitStatus {
    /**
     * Indicates that a resource transaction succeeded.
     */
    SUCCEEDED,

    /**
     * Indicates that a resource transaction failed due to an allocation conflict.
     */
    FAILED_ALLOCATION,

    /**
     * Indicates that a resource transaction failed due to a concurrent transaction holding a lock on shared keys.
     * Transactions failed with this status can be retried.
     */
    FAILED_CONCURRENT_TRANSACTION,
}
