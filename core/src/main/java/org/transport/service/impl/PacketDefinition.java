package org.transport.service.impl;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public final class PacketDefinition<T extends MessageLite> {

    private final int id;

    private final Parser<T> parser;

    public PacketDefinition(
            int id,
            Parser<T> parser
    ) {
        this.id = id;
        this.parser = parser;

    }

    public int id() {
        return id;
    }

    public Parser<T> parser() {
        return parser;
    }
}
