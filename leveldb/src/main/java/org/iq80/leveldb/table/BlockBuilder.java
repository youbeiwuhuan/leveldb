/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
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
package org.iq80.leveldb.table;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.iq80.leveldb.util.IntVector;
import org.iq80.leveldb.util.VariableLengthQuantity;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Comparator;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

public class BlockBuilder
{
    private final ChannelBuffer buffer;
    private final int blockRestartInterval;
    private final IntVector restartPositions;
    private final Comparator<byte[]> comparator;

    private int entryCount;
    private int restartBlockEntryCount;

    private boolean finished;
    private byte[] lastKey = new byte[0];

    public BlockBuilder(ChannelBuffer buffer, int blockRestartInterval, Comparator<byte[]> comparator)
    {
        Preconditions.checkNotNull(buffer, "buffer is null");
        Preconditions.checkArgument(blockRestartInterval >= 0, "blockRestartInterval is negative");
        Preconditions.checkNotNull(comparator, "comparator is null");

        this.buffer = buffer;
        this.blockRestartInterval = blockRestartInterval;
        this.comparator = comparator;

        restartPositions = new IntVector(32);
        restartPositions.add(0);  // first restart point must be 0
    }

    public void reset()
    {
        buffer.clear();
        entryCount = 0;
        restartPositions.clear();
        restartPositions.add(0); // first restart point must be 0
        restartBlockEntryCount = 0;
        lastKey = new byte[0];
        finished = false;
    }

    public int getEntryCount()
    {
        return entryCount;
    }

    public boolean isEmpty()
    {
        return entryCount == 0;
    }

    public int currentSizeEstimate()
    {
        // no need to estimate if closed
        if (finished) {
            return buffer.readableBytes();
        }

        // no records is just a single int
        if (buffer.readableBytes() == 0) {
            return SIZE_OF_INT;
        }

        return buffer.readableBytes() +                    // raw data buffer
                restartPositions.size() * SIZE_OF_INT +    // restart positions
                SIZE_OF_INT;                               // restart position size
    }

    public void add(BlockEntry blockEntry)
    {
        Preconditions.checkNotNull(blockEntry, "blockEntry is null");
        add(blockEntry.getKey(), blockEntry.getValue());
    }

    public void add(byte[] key, ChannelBuffer value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkState(!finished, "block is finished");
        Preconditions.checkPositionIndex(restartBlockEntryCount, blockRestartInterval);

        if (lastKey.length > 0 && comparator.compare(key, lastKey) <= 0) {
            Preconditions.checkArgument(lastKey.length == 0 || comparator.compare(key, lastKey) > 0, "key %s must be greater than last key %s", new String(key, UTF_8), new String(lastKey, UTF_8));
        }

        int sharedKeyBytes = 0;
        if (restartBlockEntryCount < blockRestartInterval) {
            sharedKeyBytes = calculateSharedBytes(key, lastKey);
        }
        else {
            // restart prefix compression
            restartPositions.add(buffer.readableBytes());
            restartBlockEntryCount = 0;
        }

        int nonSharedKeyBytes = key.length - sharedKeyBytes;

        // write "<shared><non_shared><value_size>"
        VariableLengthQuantity.packInt(sharedKeyBytes, buffer);
        VariableLengthQuantity.packInt(nonSharedKeyBytes, buffer);
        VariableLengthQuantity.packInt(value.readableBytes(), buffer);

        // write non-shared key bytes
        buffer.writeBytes(key, sharedKeyBytes, nonSharedKeyBytes);

        // write value bytes
        buffer.writeBytes(value, 0, value.readableBytes());

        // update last key
        // todo copy key?
        lastKey = key;

        // update state
        entryCount++;
        restartBlockEntryCount++;
    }

    public static int calculateSharedBytes(byte[] leftKey, byte[] rightKey)
    {
        int sharedKeyBytes = 0;

        if (leftKey != null && rightKey != null) {
            int minSharedKeyBytes = Ints.min(leftKey.length, rightKey.length);
            while (sharedKeyBytes < minSharedKeyBytes && leftKey[sharedKeyBytes] == rightKey[sharedKeyBytes]) {
                sharedKeyBytes++;
            }
        }

        return sharedKeyBytes;
    }

    public ChannelBuffer finish()
    {
        if (!finished) {
            finished = true;

            if (entryCount > 0) {
                restartPositions.write(buffer);
                buffer.writeInt(restartPositions.size());
            }
            else {
                buffer.writeInt(0);
            }
        }
        return buffer.slice();

    }
}
