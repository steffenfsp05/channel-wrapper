package org.transport.dispatch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.pytenix.proto.generated.TransportPackets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;

public final class BatchEncoder {

    private BatchEncoder() {
    }

/*
    public static ByteBuf encode(
            Queue<OutgoingPacket> queue,
            int maxPackets
    ) {

        TransportPackets.PacketBatch.Builder batch =
                TransportPackets.PacketBatch.newBuilder();

        int processed = 0;

        while (processed < maxPackets) {

            OutgoingPacket packet =
                    queue.poll();

            if (packet == null) {
                break;
            }

            try {

                batch.addPackets(
                        packet.envelope()
                );

                processed++;

            } catch (Exception ex) {

                ex.printStackTrace();
            }
        }

        if (processed == 0) {
            return null;
        }

        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();

        try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer)) {
            batch.build().writeTo(outputStream);
            return buffer.array();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

 */

}
