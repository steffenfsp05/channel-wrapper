package org.transport;

import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
public final class TransportOptions {


    @Builder.Default
    private final boolean batchingEnabled = true;


    @Builder.Default
    private final long batchingIntervalMs = 50L;


    @Builder.Default
    private final int maxBatchSize = 512;


    @Builder.Default
    private final int maxPendingPackets = 10_000;


    @Builder.Default
    private final int maxOutboundPackets = 50_000;

    @Builder.Default private final int maxPayloadSize = 30_000;

    @Builder.Default
    private final int dispatcherThreads =
            Math.max(
                    1,
                    Runtime.getRuntime()
                            .availableProcessors() / 2
            );

}