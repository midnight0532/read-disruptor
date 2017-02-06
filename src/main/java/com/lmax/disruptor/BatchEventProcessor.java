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


/**
 * Convenience class for handling the batching semantics of consuming entries from a {@link RingBuffer}
 * and delegating the available events to an {@link EventHandler}.
 * <p>
 * If the {@link EventHandler} also implements {@link LifecycleAware} it will be notified just after the thread
 * is started and just before the thread is shutdown.
 *
 * @param <T> event implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public final class BatchEventProcessor<T>
    implements EventProcessor
{
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExceptionHandler<? super T> exceptionHandler = new FatalExceptionHandler();
    private final DataProvider<T> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final EventHandler<? super T> eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final TimeoutHandler timeoutHandler;

    /**
     * Construct a {@link EventProcessor} that will automatically track the progress by updating its sequence when
     * the {@link EventHandler#onEvent(Object, long, boolean)} method returns.
     *
     * @param dataProvider    to which events are published.
     * @param sequenceBarrier on which it is waiting.
     * @param eventHandler    is the delegate to which events are dispatched.
     */
    public BatchEventProcessor(
        final DataProvider<T> dataProvider,
        final SequenceBarrier sequenceBarrier,
        final EventHandler<? super T> eventHandler)
    {
        this.dataProvider = dataProvider;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;

        if (eventHandler instanceof SequenceReportingEventHandler)
        {
            ((SequenceReportingEventHandler<?>) eventHandler).setSequenceCallback(sequence);
        }

        timeoutHandler = (eventHandler instanceof TimeoutHandler) ? (TimeoutHandler) eventHandler : null;
    }

    @Override
    public Sequence getSequence()
    {
        return sequence;
    }

    @Override
    public void halt()
    {
        running.set(false);
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning()
    {
        return running.get();
    }

    /**
     * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the {@link BatchEventProcessor}
     *
     * @param exceptionHandler to replace the existing exceptionHandler.
     */
    public void setExceptionHandler(final ExceptionHandler<? super T> exceptionHandler)
    {
        if (null == exceptionHandler)
        {
            throw new NullPointerException();
        }

        this.exceptionHandler = exceptionHandler;
    }

    /**
     * It is ok to have another thread rerun this method after a halt().
     *
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run()
    {
        if (!running.compareAndSet(false, true))
        {
            throw new IllegalStateException("Thread is already running");
        }
        //TODO	清除警告标志
        sequenceBarrier.clearAlert();
        //执行启动通知方法（handler实现了lifecycleaware接口）
        notifyStart();

        T event = null;
        //获取下一个序号
        long nextSequence = sequence.get() + 1L;
        try
        {	//进入死循环执行
            while (true)
            {
                try
                {//按照配置的等待策略，等待并获取当前最大可用序号
                    final long availableSequence = sequenceBarrier.waitFor(nextSequence);
                    //如果目标序号小于等于当前最大可用序号、
                    while (nextSequence <= availableSequence)
                    {	//按下一个序号获取下一个事件
                        event = dataProvider.get(nextSequence);
                        //执行处理方法
                        eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                        //下一个序号增加
                        nextSequence++;
                    }
                    //将所有当前可消费序号消费完后，更新自己的序号为最后一个消费完成的序号
                    sequence.set(availableSequence);
                }
                catch (final TimeoutException e)
                {//如果捕获到过期异常，实用配置的过期handler执行异常处理
                    notifyTimeout(sequence.get());
                }
                catch (final AlertException ex)
                {
                    if (!running.get())
                    {
                        break;
                    }
                }
                catch (final Throwable ex)
                {
                	//如果捕获到其他异常，使用配置的exceptionhandler执行异常处理
                    exceptionHandler.handleEventException(ex, nextSequence, event);
                    //更新自己的序号为最后一个消费完成的序号
                    sequence.set(nextSequence);
                    //下一个序号增加
                    nextSequence++;
                }
            }
        }
        finally
        {//执行结束通知方法（handler实现了lifecycleaware接口）
            notifyShutdown();
            running.set(false);
        }
    }

    private void notifyTimeout(final long availableSequence)
    {
        try
        {
            if (timeoutHandler != null)
            {
                timeoutHandler.onTimeout(availableSequence);
            }
        }
        catch (Throwable e)
        {
            exceptionHandler.handleEventException(e, availableSequence, null);
        }
    }

    /**
     * Notifies the EventHandler when this processor is starting up
     */
    private void notifyStart()
    {//如果handler是能觉察生命周期的handler，既实现了LifecycleAware接口，则调用其onstart方法
        if (eventHandler instanceof LifecycleAware)
        {
            try
            {
                ((LifecycleAware) eventHandler).onStart();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnStartException(ex);
            }
        }
    }

    /**
     * Notifies the EventHandler immediately prior to this processor shutting down
     */
    private void notifyShutdown()
    {
        if (eventHandler instanceof LifecycleAware)
        {
            try
            {
                ((LifecycleAware) eventHandler).onShutdown();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnShutdownException(ex);
            }
        }
    }
}