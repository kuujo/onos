/*
 * Copyright 2015-present Open Networking Foundation
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
package org.onosproject.store.cluster.messaging;

import java.util.Objects;

import com.google.common.base.MoreObjects;
import org.onlab.packet.IpAddress;
import org.onosproject.core.Version;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Representation of a TCP/UDP communication end point.
 */
public final class Endpoint {

    /**
     * Returns a new version agnostic endpoint.
     *
     * @param host the endpoint host
     * @param port the endpoint port
     * @return a new version agnostic endpoint
     */
    public static Endpoint endpoint(IpAddress host, int port) {
        return new Endpoint(host, port);
    }

    /**
     * Returns a new versioned endpoint.
     *
     * @param host the endpoint host
     * @param port the endpoint port
     * @return a new versioned endpoint
     */
    public static Endpoint versioned(IpAddress host, int port) {
        return new Endpoint(host, port, Version.NONE);
    }

    /**
     * Returns a new versioned endpoint.
     *
     * @param host the endpoint host
     * @param port the endpoint port
     * @param version the endpoint version
     * @return a new versioned endpoint
     */
    public static Endpoint versioned(IpAddress host, int port, Version version) {
        return new Endpoint(host, port, version);
    }

    private final int port;
    private final IpAddress ip;
    private final Version version;

    public Endpoint(IpAddress host, int port) {
        this(host, port, null);
    }

    public Endpoint(IpAddress host, int port, Version version) {
        this.ip = checkNotNull(host);
        this.port = port;
        this.version = version;
    }

    public IpAddress host() {
        return ip;
    }

    public int port() {
        return port;
    }

    public Version version() {
        return version;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("ip", ip)
                .add("port", port)
                .add("version", version)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port, version);
    }

    /**
     * Returns whether the given object is equal to this object, ignoring the endpoint version.
     *
     * @param object the object to compare
     * @return whether the given object matches this endpoint, ignoring the endpoint version
     */
    public boolean equalsIgnoreVersion(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        Endpoint that = (Endpoint) object;
        return this.port == that.port && Objects.equals(this.ip, that.ip);
    }

    @Override
    public boolean equals(Object object) {
        return equalsIgnoreVersion(object) && Objects.equals(this.version, ((Endpoint) object).version);
    }
}
