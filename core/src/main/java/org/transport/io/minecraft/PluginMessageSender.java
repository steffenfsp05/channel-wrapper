package org.transport.io.minecraft;

import io.netty.buffer.ByteBuf;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface PluginMessageSender<C> extends BiConsumer<C, ByteBuf> {

    @Override
    default void accept(C channel, ByteBuf buf) {
        try {
            int length = buf.readableBytes();
            byte[] data = new byte[length];
            buf.readBytes(data);
            handle(channel, data);
        } finally {
            buf.release();
        }
    }

    void handle(C channel, byte[] data);
}
