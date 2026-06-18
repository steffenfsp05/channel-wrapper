package org.transport.service;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;

import java.util.function.BiConsumer;

public interface IPacketService<C> {

    <T extends MessageLite> void addPacket(
            int packetId,
            Parser<T> parser,
            BiConsumer<PacketContext<C>, T> handler
    );

    void handleIncomingData(
            PacketContext<C> context,
            ByteBuf payload
    );

}
