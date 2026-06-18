package org.transport.dispatch;

import org.transport.TransportOptions;
import org.transport.TransportService;
import org.transport.connection.TransportConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public final class BatchDispatcher<C> implements AutoCloseable {

    private final ScheduledExecutorService scheduler;
    private final TransportService<C> transportService;
    private final TransportOptions options;

    public BatchDispatcher(TransportService<C> transportService, TransportOptions options) {
        this.transportService = transportService;
        this.options = options;

        this.scheduler = Executors.newScheduledThreadPool(
                options.getDispatcherThreads(),
                new DispatcherThreadFactory()
        );

        start();
    }

    private void start() {
        scheduler.scheduleAtFixedRate(
                this::flush,
                options.getBatchingIntervalMs(),
                options.getBatchingIntervalMs(),
                TimeUnit.MILLISECONDS
        );
    }

    private void flush() {
        try {
            List<TransportConnection<C>> connections = new ArrayList<>(
                    transportService.getConnectionManager().all()
            );

            if (connections.isEmpty()) {
                return;
            }

            for (TransportConnection<C> connection : connections) {
                if (!connection.isReady()) {
                    continue;
                }
                transportService.flush(connection);
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }


    public void triggerFlush(TransportConnection<C> connection) {
        scheduler.execute(() -> {
            if (connection.isReady()) {
                transportService.flush(connection);
            }
        });
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    private static final class DispatcherThreadFactory implements ThreadFactory {
        private final ThreadFactory delegate = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = delegate.newThread(runnable);
            thread.setDaemon(true);
            thread.setName("transport-dispatcher");
            return thread;
        }
    }
}