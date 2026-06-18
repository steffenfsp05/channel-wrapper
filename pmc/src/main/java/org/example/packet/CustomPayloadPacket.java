package org.example.packet;

public record CustomPayloadPacket(String channel, byte[] data) {
}

