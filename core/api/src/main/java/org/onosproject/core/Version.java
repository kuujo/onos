/*
 * Copyright 2014-present Open Networking Laboratory
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
package org.onosproject.core;

import java.util.Objects;

import com.google.common.collect.ComparisonChain;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Integer.parseInt;

/**
 * Representation of the product version.
 */
public final class Version implements Comparable<Version> {

    public static final String FORMAT_MINIMAL = "%d.%d";
    public static final String FORMAT_SHORT = "%d.%d.%s";
    public static final String FORMAT_LONG = "%d.%d.%s.%s";

    public static final Version UNKNOWN = new Version(0, 0, null, null);

    private static final String NEGATIVE = "Version segment cannot be negative";
    public static final String TOO_SHORT = "Version must have at least major and minor numbers";

    private final int major;
    private final int minor;
    private final String patch;
    private final String build;

    private final String format;

    // Creates a new version descriptor
    private Version(int major, int minor, String patch, String build) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.build = build;
        this.format =
                isNullOrEmpty(patch) ?
                        String.format(FORMAT_MINIMAL, major, minor) :
                        (isNullOrEmpty(build) ?
                                String.format(FORMAT_SHORT, major, minor, patch) :
                                String.format(FORMAT_LONG, major, minor, patch, build));
    }


    /**
     * Creates a new version from the specified constituent numbers.
     *
     * @param major major version number
     * @param minor minor version number
     * @param patch version patch segment
     * @param build optional build string
     * @return version descriptor
     */
    public static Version version(int major, int minor, String patch, String build) {
        checkArgument(major >= 0, NEGATIVE);
        checkArgument(minor >= 0, NEGATIVE);
        return new Version(major, minor, patch, build);
    }

    /**
     * Creates a new version by parsing the specified string.
     *
     * @param string version string
     * @return version descriptor
     */
    public static Version version(String string) {
        String[] fields = string.split("[.-]", 4);
        checkArgument(fields.length >= 2, TOO_SHORT);
        return new Version(parseInt(fields[0]), parseInt(fields[1]),
                           fields.length >= 3 ? fields[2] : null,
                           fields.length >= 4 ? fields[3] : null);
    }

    /**
     * Returns the major version number.
     *
     * @return major version number
     */
    public int major() {
        return major;
    }

    /**
     * Returns the minor version number.
     *
     * @return minor version number
     */
    public int minor() {
        return minor;
    }

    /**
     * Returns the version patch segment.
     *
     * @return patch number
     */
    public String patch() {
        return patch;
    }

    /**
     * Returns the version build string.
     *
     * @return build string
     */
    public String build() {
        return build;
    }

    private static int parseElement(String element) {
        if (element != null) {
            try {
                return Integer.parseInt(element);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    @Override
    public int compareTo(Version other) {
        return ComparisonChain.start()
                .compare(major, other.major)
                .compare(minor, other.minor)
                .compare(parseElement(patch), parseElement(other.patch))
                .compare(parseElement(build), parseElement(other.build))
                .result();
    }

    /**
     * Tests if this version is newer than the specified version.
     *
     * @param other version to compare against
     * @return true if this instance is newer
     */
    public boolean isGreaterThan(Version other) {
        return compareTo(checkNotNull(other)) > 0;
    }

    /**
     * Tests if this version is older than the specified version.
     *
     * @param other version to compare against
     * @return true if this instance is older
     */
    public boolean isLessThan(Version other) {
        return compareTo(checkNotNull(other)) < 0;
    }

    /**
     * Tests if this version is equal to the specified version.
     *
     * @param other version to compare against
     * @return true if the instances are equivalent
     */
    public boolean isEqualTo(Version other) {
        return compareTo(checkNotNull(other)) == 0;
    }

    @Override
    public String toString() {
        return format;
    }

    @Override
    public int hashCode() {
        return format.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Version) {
            final Version other = (Version) obj;
            return Objects.equals(this.format, other.format);
        }
        return false;
    }
}
