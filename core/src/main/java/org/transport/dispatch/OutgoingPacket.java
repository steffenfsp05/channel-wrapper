package org.transport.dispatch;

import org.pytenix.proto.generated.TransportPackets;

public record OutgoingPacket(
        TransportPackets.PacketEnvelope envelope,
        long timestamp
) {



    public static OutgoingPacket create(
            TransportPackets.PacketEnvelope payload
    ) {
        return new OutgoingPacket(
                payload,
                System.currentTimeMillis()
        );
    }
}