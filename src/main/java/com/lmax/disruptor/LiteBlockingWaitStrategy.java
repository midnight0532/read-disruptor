/*
 * Copyright 2011 LMAX Ltd.
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 轻量级阻塞策略，当的确需要唤醒消费线程时，再唤醒
 * Variation of the {@link BlockingWaitStrategy} that attempts to elide conditional wake-ups when
 * the lock is uncontended.  Shows performance improvements on microbenchmarks.  However this
 * wait strategy should be considered experimental as I have not full proved the correctness of
 * the lock elision code.
 */
public final class LiteBlockingWaitStrategy implements WaitStrategy
{
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();
    private final AtomicBoolean signalNeeded = new AtomicBoolean(false);

    @Override
    public long waitFor(long sequence, Sequence cursorSequence, Sequence dependentSequence, SequenceBarrier barrier)
        throws AlertException, InterruptedException
    {
        long availableSequence;
        //先过滤ringbuffer的序号是否符合消费要求。
        //如果ringbuffer的当前最大序号小于目标序号
        if (cursorSequence.get() < sequence)
        {
            lock.lock();

            try
            {
                do
                {
                    signalNeeded.getAndSet(true);
                    //如果ringbuffer的序号大于等于目标序号，跳出循环
                    if (cursorSequence.get() >= sequence)
                    {
                        break;
                    }
                    //检查disruptor关闭标识
                    barrier.checkAlert();
                    //阻塞，直到ringbuffer的当前最大序号大于等于目标序号，释放锁
                    processorNotifyCondition.await();
                }
                while (cursorSequence.get() < sequence);
            }
            finally
            {
                lock.unlock();
            }
        }
        //再过滤依赖序号是否符合消费要求
        //死循环将依赖序号赋值给可用序号，直到依赖序号大于等于目标序号跳出循环
        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            barrier.checkAlert();
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {	//如果序号唤醒消费线程，则唤醒，并设置signalNeeded为false
        if (signalNeeded.getAndSet(false))
        {
            lock.lock();
            try
            {
                processorNotifyCondition.signalAll();
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    @Override
    public String toString()
    {
        return "LiteBlockingWaitStrategy{" +
            "processorNotifyCondition=" + processorNotifyCondition +
            '}';
    }
}
