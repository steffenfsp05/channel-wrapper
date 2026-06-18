package org.transport.service;

import java.util.function.BiConsumer;

public interface PacketReceiveConsumer<C,T> extends BiConsumer<PacketContext<C>, T> { }
