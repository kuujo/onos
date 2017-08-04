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

import java.util.HashMap;
import java.util.Map;

import com.google.common.hash.Hashing;
import org.junit.Test;

/**
 *
 */
public class ConsistentHashTest {
    private static final int NUM_BUCKETS = 128;

    private static final String[] KEYS = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
    private int numPartitions = 3;
    private Map<String, Integer> mappings = new HashMap<>();

    @Test
    public void testConsistentHash() {
        for (String key : KEYS) {
            int partition = getPartition(key, numPartitions);
            mappings.put(key, partition);
        }

        for (int i = 0; i < 2; i++) {
            addPartition();
        }

        for (int i = 0; i < 2; i++) {
            removePartition();
        }
    }

    private void addPartition() {
        System.out.println("Adding partition " + (numPartitions + 1));
        numPartitions++;
        printChanges();
    }

    private void removePartition() {
        System.out.println("Removing partition " + numPartitions);
        numPartitions--;
        printChanges();
    }

    private void printChanges() {
        int numChanged = 0;
        Map<String, Integer> newMappings = new HashMap<>();
        for (String key : KEYS) {
            int partition = getPartition(key, numPartitions);
            int oldPartition = mappings.get(key);
            if (partition != oldPartition) {
                System.out.println(key + " moved from partition " + oldPartition + " to partition " + partition);
                numChanged++;
            }
            newMappings.put(key, partition);
        }
        System.out.println(((numChanged / (double) KEYS.length) * 100) + " percent of keys changed partitions");
        mappings = newMappings;
    }

    private int getPartition(String key, int partitions) {
        int bucket = Math.abs(Hashing.murmur3_32().hashUnencodedChars(key).asInt()) % NUM_BUCKETS;
        return Hashing.consistentHash(bucket, partitions) + 1;
    }
}
