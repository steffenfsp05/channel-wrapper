package org.transport.service;

import com.google.protobuf.MessageLite;
import org.transport.service.impl.PacketDefinition;

public interface PacketContext<C> {
    C getConnection();
    <T extends MessageLite> String reply(PacketDefinition<T> packetDefinition, MessageLite responsePacket);
}