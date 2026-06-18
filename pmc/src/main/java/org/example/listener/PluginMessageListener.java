package org.example.listener;

import org.example.event.PluginMessageEvent;

public interface PluginMessageListener {
    void onMessageReceived(PluginMessageEvent event);
}
