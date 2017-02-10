/*
 * Copyright 2012 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import static java.util.Arrays.copyOf;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Provides static methods for managing a {@link SequenceGroup} object.
 */
class SequenceGroups
{
    static <T> void addSequences(
        final T holder,
        final AtomicReferenceFieldUpdater<T, Sequence[]> updater,
        final Cursored cursor,
        final Sequence... sequencesToAdd)
    {
        long cursorSequence;
        Sequence[] updatedSequences;
        Sequence[] currentSequences;
        //执行循环
        do
        {
            currentSequences = updater.get(holder);//反射获取当前sequencer中的sequence数组
            updatedSequences = copyOf(currentSequences, currentSequences.length + sequencesToAdd.length);//将当前sequence数组拷贝到新数组中
            cursorSequence = cursor.getCursor();//获取ringbuffer当前最大生产序号

            int index = currentSequences.length;//获取当前sequence数组长度
            for (Sequence sequence : sequencesToAdd)
            {
                sequence.set(cursorSequence);//使用当前最大成山序号，初始化新增消费sequence的序号值
                updatedSequences[index++] = sequence;//将新sequence添加到新sequence数组中
            }
        }
        while (!updater.compareAndSet(holder, currentSequences, updatedSequences));//当sequencer中的sequence数组与循环中获得的当前数组一样时，设置新数组并跳出循环（防多线程and、then干扰？）
        //TODO	以下重复？
        cursorSequence = cursor.getCursor();//获取ringbuffer当前最大生产序号
        for (Sequence sequence : sequencesToAdd)
        {	//使用当前最大成山序号，初始化新增消费sequence的序号值
            sequence.set(cursorSequence);
        }
    }

    static <T> boolean removeSequence(
        final T holder,
        final AtomicReferenceFieldUpdater<T, Sequence[]> sequenceUpdater,
        final Sequence sequence)
    {
        int numToRemove;
        Sequence[] oldSequences;
        Sequence[] newSequences;
        //进入循环
        do
        {	//获取ringbuffer的sequence数组
            oldSequences = sequenceUpdater.get(holder);
            //获取ringbuffer的sequence数组中与上游序号屏障相同引用的数量
            numToRemove = countMatching(oldSequences, sequence);
            //如果ringbuffer的sequence数组中不存在上游序号屏障sequence，跳出循环
            if (0 == numToRemove)
            {
                break;
            }
            
            final int oldSize = oldSequences.length;//获得ringbuffer中sequence数组的长度
            newSequences = new Sequence[oldSize - numToRemove];//用原长度减要删除的sequence长度定义新sequence数组
            //循环将不需删除的sequence添加到新数组中
            for (int i = 0, pos = 0; i < oldSize; i++)
            {
                final Sequence testSequence = oldSequences[i];
                if (sequence != testSequence)
                {
                    newSequences[pos++] = testSequence;
                }
            }
        }
        while (!sequenceUpdater.compareAndSet(holder, oldSequences, newSequences));//当比较获取到的原sequence数组与sequencer中当前sequence数组相同时，设置新sequence数组并跳出循环（排除其他线程添加handler的干扰）

        return numToRemove != 0;
    }

    /**
     * 获取引用数组中指定引用的数量
     * @param values
     * @param toMatch
     * @return
     */
    private static <T> int countMatching(T[] values, final T toMatch)
    {
        int numToRemove = 0;
        for (T value : values)
        {
            if (value == toMatch) // Specifically uses identity
            {
                numToRemove++;
            }
        }
        return numToRemove;
    }
}
