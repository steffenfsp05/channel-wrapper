package org.transport.io.minecraft;

import io.netty.buffer.Unpooled;
import org.transport.TransportService;

@FunctionalInterface
public interface PluginMessageReceiver<C> {

    void handle(C channel, byte[] data);


    static <C> PluginMessageReceiver<C> zeroCopyBridge(TransportService<C> service) {
        return (channel, data) -> service.onReceiveRaw(channel, Unpooled.wrappedBuffer(data));
    }


    static <C> PluginMessageReceiver<C> autoConnectBridge(TransportService<C> service) {
        return (channel, data) -> {
            service.connect(channel);
            service.ready(channel);
            service.onReceiveRaw(channel, Unpooled.wrappedBuffer(data));
        };
    }
}