package org.transport.service.impl;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import org.transport.service.PacketContext;

import java.util.function.BiConsumer;

public final class RegisteredPacket<C> {

    private final Parser<? extends MessageLite> parser;

    private final BiConsumer<
            PacketContext<C>,
            MessageLite
            > handler;

    public RegisteredPacket(
            Parser<? extends MessageLite> parser,
            BiConsumer<
                    PacketContext<C>,
                    MessageLite
                    > handler
    ) {
        this.parser = parser;
        this.handler = handler;
    }

    public Parser<? extends MessageLite> getParser() {
        return parser;
    }

    public BiConsumer<
            PacketContext<C>,
            MessageLite
            > getHandler() {
        return handler;
    }
}