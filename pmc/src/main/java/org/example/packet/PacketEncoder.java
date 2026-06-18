package org.example.packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class PacketEncoder extends MessageToByteEncoder<CustomPayloadPacket> {
    @Override
    protected void encode(ChannelHandlerContext ctx, CustomPayloadPacket msg, ByteBuf out) {
        // Kanal-Namen als UTF-8 schreiben (Länge + String)
        byte[] channelBytes = msg.channel().getBytes(StandardCharsets.UTF_8);
        out.writeInt(channelBytes.length);
        out.writeBytes(channelBytes);

        // Payload-Daten schreiben (Länge + Byte-Array)
        out.writeInt(msg.data().length);
        out.writeBytes(msg.data());
    }
}
