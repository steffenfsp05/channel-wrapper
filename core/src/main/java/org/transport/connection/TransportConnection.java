package org.transport.connection;

import lombok.Getter;
import lombok.Setter;
import org.transport.dispatch.OutgoingPacket;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.jctools.queues.MpscArrayQueue;

public final class TransportConnection<C> {


    @Getter
    private final C handle;


    @Setter
    @Getter
    private volatile ConnectionState state;

    private final Queue<OutgoingPacket> outboundQueue;
    private final Queue<OutgoingPacket> pendingQueue;


    @Getter
    private final AtomicLong sentPackets;


    @Getter
    private final AtomicLong receivedPackets;


    @Getter
    private final long createdAt;

    public TransportConnection(C handle) {

        this.handle = handle;

        this.state = ConnectionState.CONNECTING;

        this.pendingQueue = new MpscArrayQueue<>(10_000);

        this.outboundQueue = new MpscArrayQueue<>(50_000);

        this.sentPackets = new AtomicLong();

        this.receivedPackets = new AtomicLong();

        this.createdAt = System.currentTimeMillis();
    }

    public Queue<OutgoingPacket> getPendingQueue() {
        return pendingQueue;
    }

    public Queue<OutgoingPacket> getOutboundQueue() {
        return outboundQueue;
    }

    public boolean isReady() {
        return state == ConnectionState.READY;
    }

    public boolean isDisconnected() {
        return state == ConnectionState.DISCONNECTED;
    }


    public void clear() {

        pendingQueue.clear();
        outboundQueue.clear();
    }

}
