package org.example.event;

import io.netty.channel.Channel;

// Das Event-Objekt
public record PluginMessageEvent(Channel origin, String channel, byte[] data) {
}
