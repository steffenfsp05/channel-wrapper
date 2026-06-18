package org.simulation;

import io.netty.channel.Channel;
import org.simulation.client.SimulatedSpigot;
import org.simulation.client.listener.SpigotPluginMessageListener;
import org.simulation.proto.generated.SimulationPackets;
import org.simulation.proxy.SimulatedProxy;
import org.simulation.proxy.listener.ProxyPluginMessageListener;
import org.transport.TransportOptions;
import org.transport.TransportService;
import org.transport.io.minecraft.PluginMessageReceiver;
import org.transport.io.minecraft.PluginMessageSender;
import org.transport.service.impl.DefaultPacketService;
import org.transport.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) throws Exception {
        new Main();
    }

    SimulatedProxy simulatedProxy;
    SimulatedSpigot simulatedSpigot;

    public final String channelName = "minecraft:brand";
    public final String SECRET = "ABC";


    private static final int BENCHMARK_PACKETS = 1000000;
    private final BenchmarkUtil benchmark = new BenchmarkUtil(BENCHMARK_PACKETS);

    public Main() throws InterruptedException {
        int port = 8000;

        startProxy(port);
        Thread.sleep(1000);

        startSpigot(port);
        Thread.sleep(1000);
    }

    private TransportService<Channel> buildTransport(PluginMessageSender<Channel> sender) {
        return TransportService.<Channel>builder()
                .packetService(new DefaultPacketService<>())
                .secret(SECRET)
                .encryptionEnabled(true)
                .options(TransportOptions.builder()
                        .batchingEnabled(true)
                        .maxBatchSize(512)
                        .batchingIntervalMs(1)
                        .maxPayloadSize(20000)
                        .build())
                .networkSender(sender)
                .build();
    }

    // ==========================================
    // SPIGOT (CLIENT)
    // ==========================================
    public void startSpigot(int port) {
        this.simulatedSpigot = new SimulatedSpigot("localhost", port);
        simulatedSpigot.getEventManager().registerListener(new SpigotPluginMessageListener());

        try {
            simulatedSpigot.connect();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        TransportService<Channel> transportService = buildTransport((channel, data) ->
                simulatedSpigot.sendPluginMessage(channel, channelName, data)
        );

        PluginMessageReceiver<Channel> receiver = PluginMessageReceiver.zeroCopyBridge(transportService);

        transportService.registerPacket(Packets.TRANSLATION_REQUEST, (context, request) -> {});

        transportService.registerPacket(Packets.TRANSLATION_RESULT, (context, result) -> {
            UUID id = UuidUtil.fromByteString(result.getRequestId());
            benchmark.recordReply(id);
        });

        simulatedSpigot.getEventManager().registerListener(event -> {
            receiver.handle(event.origin(), event.data());
        });

        transportService.connect(simulatedSpigot.getChannel());
        transportService.ready(simulatedSpigot.getChannel());

        StringBuilder stringBuilder = new StringBuilder("adasdas");

        System.out.println("[BENCHMARK] Starte Payload-Belastungstest mit " + BENCHMARK_PACKETS + " Paketen...");
        benchmark.start();

        for (int i = 0; i < BENCHMARK_PACKETS; i++) {
            UUID requestId = UUID.randomUUID();

            SimulationPackets.TranslationRequest request = SimulationPackets.TranslationRequest.newBuilder()
                    .setRequestId(UuidUtil.toByteString(requestId))
                    .setModule("gui")
                    .setText(stringBuilder.toString())
                    .setTargetLang("en-en")
                    .build();

            benchmark.recordSend(requestId);
            transportService.send(simulatedSpigot.getChannel(), Packets.TRANSLATION_REQUEST, request);
        }

        System.out.println("[BENCHMARK] Alle Pakete in die Outbound-Queue geschoben. Warte auf asynchrone Antworten...");

        benchmark.getCompletionFuture().join();

        benchmark.printResults();
    }

    // ==========================================
    // PROXY (SERVER)
    // ==========================================
    public void startProxy(int port) throws InterruptedException {
        this.simulatedProxy = new SimulatedProxy(port);
        simulatedProxy.getEventManager().registerListener(new ProxyPluginMessageListener());
        simulatedProxy.start();

        TransportService<Channel> transportService = buildTransport((channel, data) ->
                simulatedProxy.sendPluginMessage(channel, channelName, data)
        );

        PluginMessageReceiver<Channel> receiver = PluginMessageReceiver.autoConnectBridge(transportService);

        transportService.registerPacket(Packets.TRANSLATION_REQUEST, (context, request) -> {


                SimulationPackets.TranslationResult result = SimulationPackets.TranslationResult.newBuilder()
                        .setResult(request.getText() + " TRANSLATED!")
                        .setRequestId(request.getRequestId())
                        .build();

                context.reply(Packets.TRANSLATION_RESULT, result);

        });

        transportService.registerPacket(Packets.TRANSLATION_RESULT, (context, result) -> {});

        simulatedProxy.getEventManager().registerListener(event ->
                receiver.handle(event.origin(), event.data())
        );
    }
}