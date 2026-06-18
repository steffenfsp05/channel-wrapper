package org.transport.connection;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;


public final class ConnectionManager<C> {

    private final ConcurrentHashMap<C, TransportConnection<C>> connections =
            new ConcurrentHashMap<>();

    public TransportConnection<C> create(C handle) {

        return connections.computeIfAbsent(
                handle,
                TransportConnection::new
        );
    }

    public TransportConnection<C> get(C handle) {

        return connections.get(handle);
    }

    public void remove(C handle) {

        TransportConnection<C> connection =
                connections.remove(handle);

        if (connection != null) {

            connection.setState(
                    ConnectionState.DISCONNECTED
            );

            connection.clear();
        }
    }

    public boolean contains(C handle) {

        return connections.containsKey(handle);
    }


    public Collection<TransportConnection<C>> all() {

        return Collections.unmodifiableCollection(
                connections.values()
        );
    }


    public int size() {

        return connections.size();
    }

}