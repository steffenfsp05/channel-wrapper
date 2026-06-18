package org.transport.service.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import org.pytenix.proto.generated.TransportPackets;
import org.transport.service.IPacketService;
import org.transport.service.PacketContext;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public final class DefaultPacketService<C> implements IPacketService<C> {

    private final ConcurrentHashMap<Integer, RegisteredPacket<C>> packets = new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <T extends MessageLite> void addPacket(int packetId, Parser<T> parser, BiConsumer<PacketContext<C>, T> handler) {
        packets.put(packetId, new RegisteredPacket<>(parser, (context, msg) -> handler.accept(context, (T) msg)));
    }

    @Override
    public void handleIncomingData(PacketContext<C> context, ByteBuf payload) {
        if (payload == null || payload.readableBytes() < 1) return;

        byte packetType = payload.readByte();

        ByteBuffer nioBuffer = payload.nioBuffer(payload.readerIndex(), payload.readableBytes());

        try {
            if (packetType == 0x01) {

                TransportPackets.PacketEnvelope envelope = TransportPackets.PacketEnvelope.parseFrom(nioBuffer);
                handleEnvelope(context, envelope);

            } else if (packetType == 0x02) {

                TransportPackets.PacketBatch batch = TransportPackets.PacketBatch.parseFrom(nioBuffer);
                for (TransportPackets.PacketEnvelope envelope : batch.getPacketsList()) {
                    handleEnvelope(context, envelope);
                }

            } else {
                System.err.println("[Transport] Unbekannter Paket-Typ Header: " + packetType);
            }
        } catch (InvalidProtocolBufferException ex) {
            System.err.println("[Transport] Kritischer Fehler beim Parsen des Protobuf Payloads!");
            ex.printStackTrace();
        } finally {
            if (payload.refCnt() > 0) {
                payload.release();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleEnvelope(PacketContext<C> context, TransportPackets.PacketEnvelope envelope) {
        RegisteredPacket<C> registered = packets.get(envelope.getPacketId());
        if (registered == null) {
            System.err.println("[Transport] Unbekannte Packet ID empfangen: " + envelope.getPacketId());
            return;
        }
        try {
            MessageLite packet = registered.getParser().parseFrom(envelope.getPayload());
            registered.getHandler().accept(context, packet);
        } catch (Exception ex) {
            System.err.println("[Transport] Fehler beim Verarbeiten von Packet ID: " + envelope.getPacketId());
            ex.printStackTrace();
        }
    }

    public boolean isRegistered(int packetId) { return packets.containsKey(packetId); }
    public int getRegisteredPacketCount() { return packets.size(); }
}