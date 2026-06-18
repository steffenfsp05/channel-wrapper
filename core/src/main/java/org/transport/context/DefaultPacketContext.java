package org.transport.context;

import com.google.protobuf.MessageLite;
import org.transport.TransportService;
import org.transport.connection.TransportConnection;
import org.transport.service.PacketContext;
import org.transport.service.impl.PacketDefinition;

public final class DefaultPacketContext<C>
        implements PacketContext<C> {

    private final TransportService<C> transportService;

    private final TransportConnection<C> connection;

    public DefaultPacketContext(
            TransportService<C> transportService,
            TransportConnection<C> connection
    ) {
        this.transportService = transportService;
        this.connection = connection;
    }

    @Override
    public C getConnection() {
        return connection.getHandle();
    }

    @Override
    public <T extends MessageLite> String reply(PacketDefinition<T> packetDefinition, MessageLite responsePacket) {
        return transportService.send(
                connection.getHandle(),
                packetDefinition.id(),
                responsePacket
        );
    }

    public TransportConnection<C> getTransportConnection() {
        return connection;
    }
}