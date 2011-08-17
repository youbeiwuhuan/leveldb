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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffer;
import org.iq80.leveldb.util.Buffers;

import java.util.Arrays;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

public class InternalKey
{
    private final byte[] userKey;
    private final long sequenceNumber;
    private final ValueType valueType;

    public InternalKey(byte[] userKey, long sequenceNumber, ValueType valueType)
    {
        Preconditions.checkNotNull(userKey, "userKey is null");
        Preconditions.checkArgument(sequenceNumber >= 0, "sequenceNumber is negative");
        Preconditions.checkNotNull(valueType, "valueType is null");

        this.userKey = userKey;
        this.sequenceNumber = sequenceNumber;
        this.valueType = valueType;
    }

    public InternalKey(ChannelBuffer data)
    {
        Preconditions.checkNotNull(data, "data is null");
        if (data.readableBytes() < SIZE_OF_LONG) {
            Preconditions.checkArgument(data.readableBytes() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
        }
        this.userKey = getUserKey(data);
        this.sequenceNumber = getSequenceNumber(data);
        this.valueType = getValueType(data);
    }

    public byte[] getUserKey()
    {
        return userKey;
    }

    public long getSequenceNumber()
    {
        return sequenceNumber;
    }

    public ValueType getValueType()
    {
        return valueType;
    }

    public ChannelBuffer encode()
    {
        ChannelBuffer buffer = Buffers.buffer(userKey.length + SIZE_OF_LONG);
        buffer.writeBytes(userKey);
        buffer.writeLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));
        return buffer;
    }

    public byte[] encodeBytes() {
        return encode().array();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalKey that = (InternalKey) o;

        if (sequenceNumber != that.sequenceNumber) {
            return false;
        }
        if (userKey != null ? !Arrays.equals(userKey, that.userKey) : that.userKey != null) {
            return false;
        }
        if (valueType != that.valueType) {
            return false;
        }

        return true;
    }

    private int hash = 0;
    @Override
    public int hashCode()
    {
        if (hash == 0) {
            int result = userKey != null ? Arrays.hashCode(userKey) : 0;
            result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
            result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
            if (result == 0) {
                result = 1;
            }
            hash = result;
        }
        return hash;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("InternalKey");
        sb.append("{key=").append(new String(getUserKey(), UTF_8));      // todo don't print the real value
        sb.append(", sequenceNumber=").append(getSequenceNumber());
        sb.append(", valueType=").append(getValueType());
        sb.append('}');
        return sb.toString();
    }

    // todo find new home for these

    public static final Function<InternalKey, byte[]> INTERNAL_KEY_TO_BYTE_ARRAY = new InternalKeyToByteArrayFunction();

    public static final Function<byte[], InternalKey> BYTE_ARRAY_TO_INTERNAL_KEY = new ByteArrayToInternalKeyFunction();

    public static final Function<InternalKey, byte[]> INTERNAL_KEY_TO_USER_KEY = new InternalKeyToUserKeyFunction();

    public static Function<byte[], InternalKey> createUserKeyToInternalKeyFunction(final long sequenceNumber)
    {
        return new UserKeyInternalKeyFunction(sequenceNumber);
    }


    private static class InternalKeyToByteArrayFunction implements Function<InternalKey, byte[]>
    {
        @Override
        public byte[] apply(InternalKey internalKey)
        {
            return internalKey.encodeBytes();
        }
    }

    private static class InternalKeyToUserKeyFunction implements Function<InternalKey, byte[]>
    {
        @Override
        public byte[] apply(InternalKey internalKey)
        {
            return internalKey.getUserKey();
        }
    }

    private static class ByteArrayToInternalKeyFunction implements Function<byte[], InternalKey>
    {
        @Override
        public InternalKey apply(byte[] channelBuffer)
        {
            return new InternalKey(Buffers.wrappedBuffer(channelBuffer));
        }
    }

    private static class UserKeyInternalKeyFunction implements Function<byte[], InternalKey>
    {
        private final long sequenceNumber;

        public UserKeyInternalKeyFunction(long sequenceNumber)
        {
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public InternalKey apply(byte[] key)
        {
            return new InternalKey(key, sequenceNumber, ValueType.VALUE);
        }
    }


    private static byte[] getUserKey(ChannelBuffer data)
    {
        byte[] userKey = new byte[data.readableBytes() - SIZE_OF_LONG];
        data.getBytes(data.readerIndex(), userKey);
        return userKey;
    }

    private static long getSequenceNumber(ChannelBuffer data)
    {
        return SequenceNumber.unpackSequenceNumber(data.getLong(data.readableBytes() - SIZE_OF_LONG));
    }

    private static ValueType getValueType(ChannelBuffer data)
    {
        return SequenceNumber.unpackValueType(data.getLong(data.readableBytes() - SIZE_OF_LONG));
    }


}
