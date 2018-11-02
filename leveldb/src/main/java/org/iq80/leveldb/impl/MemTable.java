/*
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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.Slice;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;

/**
 * <p>当前活跃的表，它主要包含Log， Manifest，以及Current三个硬盘文件，以及内存中的一个跳表SkipList。
 * Log是用来记录用户的写入或者删除操作，先写入log文件(按操作的顺序)，再写入MemTable的SkipList中(根据key有序插入到相应的跳表位置)。
 * </p>
 *
 * <p>当MemTable的容量达到一定程序后，此Memtable被转换为Immutable MemTable，仍然在内存中，但可读不可写。新的MemTable被创建，
 * 并用来服务新的写入请求，至于Immutable MemTable什么时候会被写入到硬盘中，可参见数据的合并中simple compaction操作。
 * </p>
 */
public class MemTable
        implements SeekingIterable<InternalKey, Slice> {
    private final ConcurrentSkipListMap<InternalKey, Slice> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();

    public MemTable(InternalKeyComparator internalKeyComparator) {
        table = new ConcurrentSkipListMap<>(internalKeyComparator);
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    public long approximateMemoryUsage() {
        return approximateMemoryUsage.get();
    }

    public void add(long sequenceNumber, ValueType valueType, Slice key, Slice value) {
        requireNonNull(valueType, "valueType is null");
        requireNonNull(key, "key is null");
        requireNonNull(valueType, "valueType is null");

        InternalKey internalKey = new InternalKey(key, sequenceNumber, valueType);
        table.put(internalKey, value);

        approximateMemoryUsage.addAndGet(key.length() + SIZE_OF_LONG + value.length());
    }

    public LookupResult get(LookupKey key) {
        requireNonNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            return null;
        }

        InternalKey entryKey = entry.getKey();
        if (entryKey.getUserKey().equals(key.getUserKey())) {
            if (entryKey.getValueType() == ValueType.DELETION) {
                return LookupResult.deleted(key);
            } else {
                return LookupResult.ok(key, entry.getValue());
            }
        }
        return null;
    }

    @Override
    public MemTableIterator iterator() {
        return new MemTableIterator();
    }

    public class MemTableIterator
            implements InternalIterator {
        private PeekingIterator<Entry<InternalKey, Slice>> iterator;

        public MemTableIterator() {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst() {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public void seek(InternalKey targetKey) {
            iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator());
        }

        @Override
        public InternalEntry peek() {
            Entry<InternalKey, Slice> entry = iterator.peek();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public InternalEntry next() {
            Entry<InternalKey, Slice> entry = iterator.next();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
