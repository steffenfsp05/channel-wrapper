package org.simulation;

import io.netty.channel.Channel;
import org.example.packet.CustomPayloadPacket;


public abstract class Simulation {

    public abstract Channel getChannel();

    public void sendPluginMessage(Channel channel, String channelName, byte[] data) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(new CustomPayloadPacket(channelName, data));
        }
    }
}