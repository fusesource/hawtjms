/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.amqpjms.jms.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.fusesource.amqpjms.jms.message.JmsInboundMessageDispatch;

public class MessageQueue {

    protected static class QueueEntry {
        final JmsInboundMessageDispatch message;
        final int size;

        QueueEntry(JmsInboundMessageDispatch message, int size) {
            this.message = message;
            this.size = size;
        }
    }

    protected final long maxSize;
    protected final LinkedList<QueueEntry> list = new LinkedList<QueueEntry>();
    protected boolean closed;
    protected boolean running;
    protected long size;

    public MessageQueue(long maxSize) {
        this.maxSize = maxSize;
    }

    public void enqueue(JmsInboundMessageDispatch message) {
        QueueEntry entry = new QueueEntry(message, 1);
        synchronized (this) {
            list.addLast(entry);
            size += entry.size;
            this.notify();
        }
    }

    public boolean isEmpty() {
        synchronized (this) {
            return list.isEmpty();
        }
    }

    public JmsInboundMessageDispatch dequeue(long timeout) throws InterruptedException {
        synchronized (this) {
            // Wait until the consumer is ready to deliver messages.
            while (timeout != 0 && !closed && (list.isEmpty() || !running)) {
                if (timeout == -1) {
                    this.wait();
                } else {
                    this.wait(timeout);
                    break;
                }
            }
            if (closed || !running || list.isEmpty()) {
                return null;
            }
            QueueEntry entry = list.removeFirst();
            size -= entry.size;
            removed(entry);
            return entry.message;
        }
    }

    public JmsInboundMessageDispatch dequeueNoWait() {
        synchronized (this) {
            if (closed || !running || list.isEmpty()) {
                return null;
            }
            QueueEntry entry = list.removeFirst();
            size -= entry.size;
            removed(entry);
            return entry.message;
        }
    }

    protected void removed(QueueEntry entry) {
    }

    public void start() {
        synchronized (this) {
            running = true;
            this.notifyAll();
        }
    }

    public void stop() {
        synchronized (this) {
            running = false;
            this.notifyAll();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void close() {
        synchronized (this) {
            if (!closed) {
                running = false;
                closed = true;
            }
            this.notifyAll();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public int size() {
        synchronized (this) {
            return list.size();
        }
    }

    public void clear() {
        synchronized (this) {
            list.clear();
        }
    }

    public List<JmsInboundMessageDispatch> removeAll() {
        synchronized (this) {
            ArrayList<JmsInboundMessageDispatch> rc = new ArrayList<JmsInboundMessageDispatch>(list.size());
            for (QueueEntry entry : list) {
                rc.add(entry.message);
            }
            list.clear();
            size = 0;
            return rc;
        }
    }

    @Override
    public String toString() {
        synchronized (this) {
            return list.toString();
        }
    }

    public boolean isFull() {
        synchronized (this) {
            return this.size >= maxSize;
        }
    }
}
