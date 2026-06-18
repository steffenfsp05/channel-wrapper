package org.transport.chunk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.concurrent.ConcurrentHashMap;

public final class ChunkReassembler {

    private final ConcurrentHashMap<Integer, CompositeByteBuf> chunkCache = new ConcurrentHashMap<>();


    public ByteBuf processIncoming(ByteBuf payload) {
        if (payload.readableBytes() < 1) {
            payload.release();
            return null;
        }

        byte outerHeader = payload.readByte();

        if (outerHeader == 0x00) {
            ByteBuf fullPayload = payload.readRetainedSlice(payload.readableBytes());
            payload.release();
            return fullPayload;
        }

        if (outerHeader == 0x01) {
            if (payload.readableBytes() < 5) {
                payload.release();
                return null;
            }

            int msgId = payload.readInt();
            boolean isLast = payload.readByte() == 0x01;

            ByteBuf chunkData = payload.readRetainedSlice(payload.readableBytes());
            payload.release();

            CompositeByteBuf assemblyBuffer = chunkCache.computeIfAbsent(msgId,
                    ignored -> PooledByteBufAllocator.DEFAULT.compositeBuffer()
            );

            assemblyBuffer.addComponent(true, chunkData);

            if (isLast) {
                chunkCache.remove(msgId);
                return assemblyBuffer;
            }
            return null;
        }

        System.err.println("[Transport] Illegales Outer Protokoll Byte: " + outerHeader);
        payload.release();
        return null;
    }

    public void clear() {
        for (CompositeByteBuf buf : chunkCache.values()) {
            if (buf.refCnt() > 0) buf.release();
        }
        chunkCache.clear();
    }
}