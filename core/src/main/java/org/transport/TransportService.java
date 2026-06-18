package org.transport;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.Builder;
import lombok.Getter;
import org.pytenix.proto.generated.TransportPackets;
import org.transport.chunk.ChunkReassembler;
import org.transport.chunk.ChunkSplitter;
import org.transport.connection.ConnectionManager;
import org.transport.connection.ConnectionState;
import org.transport.connection.TransportConnection;
import org.transport.context.DefaultPacketContext;
import org.transport.dispatch.BatchDispatcher;
import org.transport.dispatch.OutgoingPacket;
import org.transport.service.IPacketService;
import org.transport.service.PacketContext;
import org.transport.service.impl.PacketDefinition;
import org.transport.util.CryptoUtil;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public final class TransportService<C> implements AutoCloseable {

    private final IPacketService<C> packetService;
    @Getter private final ConnectionManager<C> connectionManager;
    @Getter private final TransportOptions options;
    private final BiConsumer<C, ByteBuf> networkSender;
    private final boolean encryptionEnabled;
    private final CryptoUtil cryptoUtil;
    private final BatchDispatcher<C> batchDispatcher;
    private final ConcurrentHashMap<TransportConnection<C>, DefaultPacketContext<C>> contexts = new ConcurrentHashMap<>();
    private final Disruptor<IncomingPacketEvent<C>> disruptor;
    private final RingBuffer<IncomingPacketEvent<C>> ringBuffer;

    private final ChunkSplitter chunkSplitter = new ChunkSplitter();
    private final ChunkReassembler chunkReassembler = new ChunkReassembler();

    private final Set<TransportConnection<C>> flushingConnections = ConcurrentHashMap.newKeySet();

    @Builder
    public TransportService(IPacketService<C> packetService, TransportOptions options, boolean encryptionEnabled, String secret, BiConsumer<C, ByteBuf> networkSender) {
        this.packetService = packetService;
        this.options = options;
        this.networkSender = networkSender;
        this.connectionManager = new ConnectionManager<>();
        this.encryptionEnabled = encryptionEnabled;
        this.cryptoUtil = encryptionEnabled ? new CryptoUtil(secret) : null;
        this.batchDispatcher = new BatchDispatcher<>(this, options);

        this.disruptor = new Disruptor<>(IncomingPacketEvent::new, 65536, DaemonThreadFactory.INSTANCE);


        this.disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            try {
                if (event.connection == null || event.payload == null) return;
                ByteBuf workingPayload = event.payload;

                if (encryptionEnabled) {
                    workingPayload = cryptoUtil.secureUnwrap(workingPayload);
                    if (workingPayload == null) return;
                }

                packetService.handleIncomingData(contexts.get(event.connection), workingPayload);
            } catch (Exception ex) {
                System.err.println("[Transport] Fehler im Disruptor-Worker:");
                ex.printStackTrace();
            } finally {
                event.clear();
            }
        });
        this.ringBuffer = disruptor.start();
    }

    public void flush(TransportConnection<C> connection) {

        if (!flushingConnections.add(connection)) {
            return;
        }
        try {
            Queue<OutgoingPacket> queue = connection.getOutboundQueue();

            while (!queue.isEmpty()) {
                OutgoingPacket firstPacket = queue.poll();
                if (firstPacket == null) break;

                OutgoingPacket secondPacket = queue.poll();
                ByteBuf directBuf = null;
                ByteBuf payloadToSend = null;
                int processed = 1;

                try {
                    if (secondPacket == null) {
                        TransportPackets.PacketEnvelope envelope = firstPacket.envelope();
                        int serializedSize = envelope.getSerializedSize();

                        directBuf = PooledByteBufAllocator.DEFAULT.directBuffer(1 + serializedSize);
                        directBuf.writeByte(0x01);

                        ByteBuffer nioOut = directBuf.nioBuffer(1, serializedSize);
                        CodedOutputStream out = CodedOutputStream.newInstance(nioOut);
                        envelope.writeTo(out);
                        out.flush();
                        directBuf.writerIndex(1 + serializedSize);
                    } else {
                        TransportPackets.PacketBatch.Builder batch = TransportPackets.PacketBatch.newBuilder();
                        batch.addPackets(firstPacket.envelope());
                        batch.addPackets(secondPacket.envelope());
                        processed = 2;

                        while (processed < options.getMaxBatchSize()) {
                            OutgoingPacket packet = queue.poll();
                            if (packet == null) break;
                            batch.addPackets(packet.envelope());
                            processed++;
                        }

                        TransportPackets.PacketBatch finalBatch = batch.build();
                        int serializedSize = finalBatch.getSerializedSize();

                        directBuf = PooledByteBufAllocator.DEFAULT.directBuffer(1 + serializedSize);
                        directBuf.writeByte(0x02);

                        ByteBuffer nioOut = directBuf.nioBuffer(1, serializedSize);
                        CodedOutputStream out = CodedOutputStream.newInstance(nioOut);
                        finalBatch.writeTo(out);
                        out.flush();
                        directBuf.writerIndex(1 + serializedSize);
                    }

                    if (encryptionEnabled) {
                        payloadToSend = cryptoUtil.secureWrap(directBuf);
                        directBuf.release();
                    } else {
                        payloadToSend = directBuf;
                    }

                    chunkSplitter.splitAndSend(
                            payloadToSend,
                            options.getMaxPayloadSize(),
                            connection.getHandle(),
                            networkSender
                    );

                    connection.getSentPackets().addAndGet(processed);

                } catch (Exception e) {
                    if (directBuf != null && directBuf.refCnt() > 0) directBuf.release();
                    if (payloadToSend != null && payloadToSend.refCnt() > 0) payloadToSend.release();
                    e.printStackTrace();
                }
            }
        } finally {
            flushingConnections.remove(connection);
        }
    }

    public void onReceiveRaw(C handle, ByteBuf payload) {
        TransportConnection<C> connection = connectionManager.get(handle);
        if (connection == null) {
            payload.release();
            return;
        }

        ByteBuf completedPayload = chunkReassembler.processIncoming(payload);

        if (completedPayload == null) return;

        ringBuffer.publishEvent((event, sequence) -> {
            event.connection = connection;
            event.payload = completedPayload;
        });
    }

    public TransportConnection<C> connect(C handle) {
        TransportConnection<C> connection = connectionManager.create(handle);
        contexts.computeIfAbsent(connection, ignored -> new DefaultPacketContext<>(this, connection));
        return connection;
    }

    public void ready(C handle) {
        TransportConnection<C> connection = connectionManager.get(handle);
        if (connection == null || connection.isDisconnected() || connection.isReady()) return;
        connection.setState(ConnectionState.READY);
        flushPending(connection);
    }

    private void flushPending(TransportConnection<C> connection) {
        Queue<OutgoingPacket> pending = connection.getPendingQueue();
        OutgoingPacket packet;
        while ((packet = pending.poll()) != null) enqueue(connection, packet);
    }

    private boolean enqueue(TransportConnection<C> connection, OutgoingPacket packet) {
        return connection.getOutboundQueue().offer(packet);
    }

    public void disconnect(C handle) {
        TransportConnection<C> connection = connectionManager.get(handle);
        if (connection == null) return;
        connection.setState(ConnectionState.DISCONNECTED);
        connection.clear();
        contexts.remove(connection);
        connectionManager.remove(handle);
    }

    public <T extends MessageLite> void registerPacket(PacketDefinition<T> definition, BiConsumer<PacketContext<C>,T> handler)
    {
        packetService.addPacket(definition.id(),definition.parser(), handler);
    }

    public String send(C handle, PacketDefinition<?> definition, MessageLite packet) { return send(handle, definition.id(), packet); }

    public String send(C handle, int packetId, MessageLite packet) {
        TransportConnection<C> connection = connectionManager.get(handle);
        if (connection == null) return "connection null";
        if (connection.isDisconnected()) return "client not connected";

        TransportPackets.PacketEnvelope envelope = TransportPackets.PacketEnvelope.newBuilder()
                .setPacketId(packetId)
                .setPayload(packet.toByteString())
                .build();
        OutgoingPacket outgoing = new OutgoingPacket(envelope, System.currentTimeMillis());

        if (!connection.isReady()) {
            return connection.getPendingQueue().offer(outgoing) ? "offering to queue" : "dropped - queue full";
        }

        if (enqueue(connection, outgoing)) {
            if (connection.getOutboundQueue().size() >= options.getMaxBatchSize()) {
                batchDispatcher.triggerFlush(connection);
            }
            return "enqueued packet";
        }
        return "dropped - outbound queue full";
    }

    @Override
    public void close() {
        batchDispatcher.close();
        if (disruptor != null) disruptor.shutdown();
        for (TransportConnection<C> connection : connectionManager.all()) connection.clear();
        contexts.clear();

        chunkReassembler.clear();
    }

    private static class IncomingPacketEvent<C> {
        TransportConnection<C> connection;
        ByteBuf payload;

        public void clear() {
            this.connection = null;
            if (this.payload != null) {
                if (this.payload.refCnt() > 0) this.payload.release();
                this.payload = null;
            }
        }
    }
}