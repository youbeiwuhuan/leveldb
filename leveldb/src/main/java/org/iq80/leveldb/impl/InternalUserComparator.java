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
package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.table.UserComparator;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;

import java.nio.Buffer;

import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;

public class InternalUserComparator implements UserComparator
{
    private final InternalKeyComparator internalKeyComparator;

    public InternalUserComparator(InternalKeyComparator internalKeyComparator)
    {
        this.internalKeyComparator = internalKeyComparator;
    }

    @Override
    public int compare(byte[] left, byte[] right)
    {
        return internalKeyComparator.compare(new InternalKey(Buffers.wrappedBuffer(left)), new InternalKey(Buffers.wrappedBuffer(right)));
    }

    @Override
    public byte[] findShortestSeparator(
            byte[] start,
            byte[] limit)
    {
        // Attempt to shorten the user portion of the key
        byte[] startUserKey = new InternalKey(Buffers.wrappedBuffer(start)).getUserKey();
        byte[] limitUserKey = new InternalKey(Buffers.wrappedBuffer(limit)).getUserKey();

        byte[] shortestSeparator = internalKeyComparator.getUserComparator().findShortestSeparator(startUserKey, limitUserKey);

        if (internalKeyComparator.getUserComparator().compare(startUserKey, shortestSeparator) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(shortestSeparator, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
//            Preconditions.checkState(compare(start, newInternalKey.encode()) < 0);// todo
//            Preconditions.checkState(compare(newInternalKey.encode(), limit) < 0);// todo

            return newInternalKey.encodeBytes();
        }
        return start;
    }

    @Override
    public byte[] findShortSuccessor(byte[] key)
    {
        byte[] userKey = new InternalKey(Buffers.wrappedBuffer(key)).getUserKey();

        byte[] shortSuccessor = internalKeyComparator.getUserComparator().findShortSuccessor(userKey);

        if (internalKeyComparator.getUserComparator().compare(userKey, shortSuccessor) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(shortSuccessor, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
//            Preconditions.checkState(compare(key, newInternalKey.encode()) < 0);// todo

            return newInternalKey.encodeBytes();
        }
        return key;
    }
}
