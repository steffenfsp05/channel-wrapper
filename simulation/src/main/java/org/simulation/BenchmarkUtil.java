package org.simulation;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

public final class BenchmarkUtil {

    private final int expectedPackets;
    private final ConcurrentHashMap<UUID, Long> startTimes = new ConcurrentHashMap<>(16384);
    private final ConcurrentLinkedQueue<Long> latenciesNs = new ConcurrentLinkedQueue<>();

    private final LongAdder sentCount = new LongAdder();
    private final LongAdder receivedCount = new LongAdder();

    private long globalStartNs;
    private long globalEndNs;

    @Getter
    private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    public BenchmarkUtil(int expectedPackets) {
        this.expectedPackets = expectedPackets;
    }

    public void start() {
        this.globalStartNs = System.nanoTime();
    }

    public void recordSend(UUID id) {
        startTimes.put(id, System.nanoTime());
        sentCount.increment();
    }

    public void recordReply(UUID id) {
        Long startTime = startTimes.remove(id);
        if (startTime == null) return;

        long latency = System.nanoTime() - startTime;
        latenciesNs.add(latency);
        receivedCount.increment();

        if (receivedCount.sum() >= expectedPackets) {
            this.globalEndNs = System.nanoTime();
            completionFuture.complete(null);
        }
    }

    public void printResults() {
        long totalTimeNs = globalEndNs - globalStartNs;
        double totalTimeSec = totalTimeNs / 1_000_000_000.0;
        int totalPackets = receivedCount.intValue();

        List<Long> sortedLatencies = new ArrayList<>(latenciesNs);
        Collections.sort(sortedLatencies);

        if (sortedLatencies.isEmpty()) {
            System.err.println("Keine Daten für den Benchmark empfangen!");
            return;
        }

        long sumNs = 0;
        for (long lat : sortedLatencies) sumNs += lat;
        double avgMs = (sumNs / (double) totalPackets) / 1_000_000.0;

        double p50Ms = sortedLatencies.get((int) (totalPackets * 0.50)) / 1_000_000.0;
        double p95Ms = sortedLatencies.get((int) (totalPackets * 0.95)) / 1_000_000.0;
        double p99Ms = sortedLatencies.get((int) (totalPackets * 0.99)) / 1_000_000.0;
        double minMs = sortedLatencies.get(0) / 1_000_000.0;
        double maxMs = sortedLatencies.get(sortedLatencies.size() - 1) / 1_000_000.0;

        double throughputSend = sentCount.sum() / totalTimeSec;
        double throughputReceive = totalPackets / totalTimeSec;

        System.out.println("\n==================================================");
        System.out.println("           TRANSPORT SYSTEM BENCHMARK             ");
        System.out.println("==================================================");
        System.out.printf("Gesamtdauer:          %,.3f Sekunden\n", totalTimeSec);
        System.out.printf("Pakete gesendet:      %d\n", sentCount.sum());
        System.out.printf("Pakete empfangen:     %d\n", totalPackets);
        System.out.printf("Durchsatz (Send):     %,.2f Pakete/Sek\n", throughputSend);
        System.out.printf("Durchsatz (Receive):  %,.2f Pakete/Sek\n", throughputReceive);
        System.out.println("--------------------------------------------------");
        System.out.println("LATENZ / ROUND-TRIP-TIME (RTT):");
        System.out.printf("  Min:                %,.3f ms\n", minMs);
        System.out.printf("  Avg:                %,.3f ms\n", avgMs);
        System.out.printf("  Median (p50):       %,.3f ms\n", p50Ms);
        System.out.printf("  95th Percentile:    %,.3f ms (95%% aller Pakete schneller als das)\n", p95Ms);
        System.out.printf("  99th Percentile:    %,.3f ms (Heftigste Ausreißer)\n", p99Ms);
        System.out.printf("  Max:                %,.3f ms\n", maxMs);
        System.out.println("==================================================\n");
    }
}