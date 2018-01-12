/*
 * Copyright 2018-present Open Networking Foundation
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
package org.onosproject.cluster;

import org.onlab.util.Identifier;
import org.onosproject.core.Version;

/**
 * Membership group identifier.
 */
public class MembershipGroupId extends Identifier<Version> {

    /**
     * Returns a new membership group ID from the given version.
     *
     * @param version the version from which to return the identifier
     * @return a new membership group identifier for the given version
     */
    public static MembershipGroupId from(Version version) {
        if (version != null) {
            return new MembershipGroupId(version);
        }
        return new MembershipGroupId();
    }

    private MembershipGroupId() {
        super();
    }

    public MembershipGroupId(Version value) {
        super(value);
    }
}
