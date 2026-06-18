package org.transport.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CryptoUtil implements AutoCloseable {

    private static final int MAGIC_HEADER = 0x50595445; // "PYTE"
    private static final String AES_ALGORITHM = "AES/GCM/NoPadding";

    private static final int GCM_TAG_LENGTH_BIT = 128;
    private static final int GCM_TAG_LENGTH_BYTE = 16;
    private static final int IV_LENGTH_BYTE = 12;
    private static final long TIME_WINDOW_MS = 5000;
    private static final int MAX_CACHE_SIZE = 100_000;

    private final SecretKeySpec aesKey;
    private final SecureRandom secureRandom = new SecureRandom();

    private final ConcurrentHashMap<IvKey, Long> seenIvs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cacheCleaner;

    private final ThreadLocal<Cipher> threadLocalCipher = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance(AES_ALGORITHM);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    });

    private final ThreadLocal<ByteBuffer> threadLocalAadBuffer = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(Long.BYTES)
    );

    private final ThreadLocal<byte[]> threadLocalIvBuffer = ThreadLocal.withInitial(() ->
            new byte[IV_LENGTH_BYTE]
    );

    public CryptoUtil(String secretKey) {
        if (secretKey == null || secretKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Secret darf nicht leer sein!");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] keyBytes = digest.digest(secretKey.getBytes(StandardCharsets.UTF_8));
            this.aesKey = new SecretKeySpec(keyBytes, "AES");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 nicht verfügbar", e);
        }

        this.cacheCleaner = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable, "Crypto-Cache-Cleaner");
            t.setDaemon(true);
            return t;
        });
        this.cacheCleaner.scheduleAtFixedRate(this::cleanCacheTask, 1, 1, TimeUnit.SECONDS);
    }

    public ByteBuf secureWrap(ByteBuf payload) throws SecurityException {
        ByteBuf outBuf = null;
        try {
            byte[] iv = threadLocalIvBuffer.get();
            secureRandom.nextBytes(iv);

            long timestamp = System.currentTimeMillis();

            ByteBuffer aadBuffer = threadLocalAadBuffer.get();
            aadBuffer.clear();
            aadBuffer.putLong(timestamp);
            aadBuffer.flip();

            Cipher cipher = threadLocalCipher.get();
            cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(GCM_TAG_LENGTH_BIT, iv));
            cipher.updateAAD(aadBuffer);

            int plainLen = payload.readableBytes();
            int cipherLen = plainLen + GCM_TAG_LENGTH_BYTE;
            int totalOutLength = Integer.BYTES + Long.BYTES + IV_LENGTH_BYTE + cipherLen;

            outBuf = PooledByteBufAllocator.DEFAULT.directBuffer(totalOutLength);
            outBuf.writeInt(MAGIC_HEADER);
            outBuf.writeLong(timestamp);
            outBuf.writeBytes(iv);

            ByteBuffer inNio = payload.nioBuffer(payload.readerIndex(), plainLen);
            ByteBuffer outNio = outBuf.nioBuffer(outBuf.writerIndex(), cipherLen);

            cipher.doFinal(inNio, outNio);
            outBuf.writerIndex(outBuf.writerIndex() + cipherLen);

            return outBuf;
        } catch (Exception e) {
            if (outBuf != null && outBuf.refCnt() > 0) outBuf.release();
            throw new SecurityException("Fehler bei der Verschlüsselung", e);
        }
    }

    public ByteBuf secureUnwrap(ByteBuf data) throws SecurityException {
        if (data == null || data.readableBytes() < (4 + 8 + IV_LENGTH_BYTE + GCM_TAG_LENGTH_BYTE)) {
            throw new SecurityException("Paket zu klein.");
        }

        ByteBuf outBuf = null;
        try {
            if (data.readInt() != MAGIC_HEADER) {
                throw new SecurityException("Ungültiger Magic Header.");
            }

            long timestamp = data.readLong();
            long now = System.currentTimeMillis();

            if (Math.abs(now - timestamp) > TIME_WINDOW_MS) {
                throw new SecurityException("Zeitstempel abgelaufen.");
            }

            long ivPart1 = data.readLong();
            int ivPart2 = data.readInt();

            IvKey ivKey = new IvKey(ivPart1, ivPart2);
            if (seenIvs.putIfAbsent(ivKey, now + TIME_WINDOW_MS) != null) {
                throw new SecurityException("Replay-Angriff blockiert!");
            }

            byte[] iv = threadLocalIvBuffer.get();
            iv[0] = (byte) (ivPart1 >>> 56); iv[1] = (byte) (ivPart1 >>> 48);
            iv[2] = (byte) (ivPart1 >>> 40); iv[3] = (byte) (ivPart1 >>> 32);
            iv[4] = (byte) (ivPart1 >>> 24); iv[5] = (byte) (ivPart1 >>> 16);
            iv[6] = (byte) (ivPart1 >>> 8);  iv[7] = (byte) ivPart1;
            iv[8] = (byte) (ivPart2 >>> 24); iv[9] = (byte) (ivPart2 >>> 16);
            iv[10] = (byte) (ivPart2 >>> 8); iv[11] = (byte) ivPart2;

            Cipher cipher = threadLocalCipher.get();
            cipher.init(Cipher.DECRYPT_MODE, aesKey, new GCMParameterSpec(GCM_TAG_LENGTH_BIT, iv));

            ByteBuffer aadBuffer = threadLocalAadBuffer.get();
            aadBuffer.clear();
            aadBuffer.putLong(timestamp);
            aadBuffer.flip();
            cipher.updateAAD(aadBuffer);

            int cipherLen = data.readableBytes();
            int plainLen = cipherLen - GCM_TAG_LENGTH_BYTE;

            outBuf = PooledByteBufAllocator.DEFAULT.directBuffer(plainLen);
            ByteBuffer inNio = data.nioBuffer(data.readerIndex(), cipherLen);
            ByteBuffer outNio = outBuf.nioBuffer(outBuf.writerIndex(), plainLen);

            cipher.doFinal(inNio, outNio);
            outBuf.writerIndex(outBuf.writerIndex() + plainLen);

            return outBuf;

        } catch (AEADBadTagException e) {
            if (outBuf != null && outBuf.refCnt() > 0) outBuf.release();
            throw new SecurityException("Signatur ungültig.", e);
        } catch (Exception e) {
            if (outBuf != null && outBuf.refCnt() > 0) outBuf.release();
            throw new SecurityException("Fehler bei der Entschlüsselung.", e);
        }
    }

    private record IvKey(long part1, int part2) {}

    private void cleanCacheTask() {
        if (seenIvs.isEmpty()) return;
        long now = System.currentTimeMillis();
        seenIvs.values().removeIf(expiry -> now > expiry);
        if (seenIvs.size() > MAX_CACHE_SIZE) seenIvs.clear();
    }

    @Override
    public void close() {
        cacheCleaner.shutdownNow();
    }
}