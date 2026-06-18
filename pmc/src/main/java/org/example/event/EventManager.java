package org.example.event;

import org.example.listener.PluginMessageListener;

import java.util.ArrayList;
import java.util.List;

public class EventManager {
    private final List<PluginMessageListener> listeners = new ArrayList<>();

    public void registerListener(PluginMessageListener listener) {
        listeners.add(listener);
    }

    public void callEvent(PluginMessageEvent event) {
        for (PluginMessageListener listener : listeners) {
            listener.onMessageReceived(event);
        }
    }
}
