package org.transport.chunk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public final class ChunkSplitter {

    private final AtomicInteger messageIdGenerator = new AtomicInteger(0);

    public <C> void splitAndSend(ByteBuf payload, int maxPayloadSize, C channel, BiConsumer<C, ByteBuf> networkSender) {
        int totalBytes = payload.readableBytes();

        if (totalBytes <= maxPayloadSize - 1) {
            ByteBuf outerHeader = PooledByteBufAllocator.DEFAULT.directBuffer(1);
            outerHeader.writeByte(0x00); // 0x00 = Single Packet

            CompositeByteBuf composite = PooledByteBufAllocator.DEFAULT.compositeBuffer();
            composite.addComponent(true, outerHeader);
            composite.addComponent(true, payload);

            networkSender.accept(channel, composite);
            return;
        }

        int msgId = messageIdGenerator.getAndIncrement();
        int dataCapacityPerChunk = maxPayloadSize - 6;

        while (payload.isReadable()) {
            int chunkSize = Math.min(payload.readableBytes(), dataCapacityPerChunk);
            boolean isLast = payload.readableBytes() == chunkSize;

           ByteBuf dataSlice = payload.readRetainedSlice(chunkSize);

            ByteBuf chunkHeader = PooledByteBufAllocator.DEFAULT.directBuffer(6);
            chunkHeader.writeByte(0x01); // 0x01 = Chunked Packet
            chunkHeader.writeInt(msgId);
            chunkHeader.writeByte(isLast ? 0x01 : 0x00);

            CompositeByteBuf chunkComposite = PooledByteBufAllocator.DEFAULT.compositeBuffer();
            chunkComposite.addComponent(true, chunkHeader);
            chunkComposite.addComponent(true, dataSlice);

            networkSender.accept(channel, chunkComposite);
        }

        payload.release();
    }
}
