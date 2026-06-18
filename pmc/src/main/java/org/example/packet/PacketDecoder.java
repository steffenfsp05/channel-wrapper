package org.example.packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PacketDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Warten, bis zumindest die Länge des Kanalnamens lesbar ist
        if (in.readableBytes() < 4) return;
        in.markReaderIndex();

        int channelLength = in.readInt();
        if (in.readableBytes() < channelLength + 4) {
            in.resetReaderIndex();
            return;
        }

        // Kanal lesen
        byte[] channelBytes = new byte[channelLength];
        in.readBytes(channelBytes);
        String channel = new String(channelBytes, StandardCharsets.UTF_8);

        // Daten lesen
        int dataLength = in.readInt();
        if (in.readableBytes() < dataLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] data = new byte[dataLength];
        in.readBytes(data);

        out.add(new CustomPayloadPacket(channel, data));
    }
}
