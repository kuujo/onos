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
package org.onosproject.store.primitives.resources.impl;

import io.atomix.copycat.client.session.CopycatSession;
import org.onosproject.store.service.DistributedPrimitive;

/**
 * Abstract resource.
 */
public abstract class AbstractCopycatPrimitive implements DistributedPrimitive {
    protected final CopycatSession session;

    public AbstractCopycatPrimitive(CopycatSession session) {
        this.session = session;
    }

    @Override
    public String name() {
        return session.name();
    }
}
