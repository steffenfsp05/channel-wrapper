package org.simulation.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.Getter;
import org.simulation.Simulation;
import org.example.event.EventManager;
import org.example.event.PluginMessageEvent;
import org.example.packet.CustomPayloadPacket;
import org.example.packet.PacketDecoder;
import org.example.packet.PacketEncoder;

public class SimulatedSpigot extends Simulation {
    private final String host;
    private final int port;
    private Channel channel;
    @Getter
    private final EventManager eventManager = new EventManager();

    public SimulatedSpigot(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                        p.addLast(new LengthFieldPrepender(4));

                        p.addLast(new PacketDecoder());
                        p.addLast(new PacketEncoder());

                        p.addLast(new SimpleChannelInboundHandler<CustomPayloadPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, CustomPayloadPacket packet) {
                                eventManager.callEvent(new PluginMessageEvent(ctx.channel(), packet.channel(), packet.data()));
                            }
                        });
                    }
                });

        ChannelFuture f = b.connect(host, port).sync();
        this.channel = f.channel();
        System.out.println("[Spigot] Erfolgreich mit Proxy verbunden.");
    }

    @Override public Channel getChannel()
    {
        return channel;
    }

}
