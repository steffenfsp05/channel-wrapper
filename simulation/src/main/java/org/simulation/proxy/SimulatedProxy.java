package org.simulation.proxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.simulation.Simulation;
import org.example.event.EventManager;
import org.example.event.PluginMessageEvent;
import org.example.packet.CustomPayloadPacket;
import org.example.packet.PacketDecoder;
import org.example.packet.PacketEncoder;

public class SimulatedProxy extends Simulation {
    private final int port;
    private final EventManager eventManager = new EventManager();

    public SimulatedProxy(int port) {
        this.port = port;
    }

    Channel channel;

    public EventManager getEventManager() { return eventManager; }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                            p.addLast(new LengthFieldPrepender(4));

                            System.out.println("mit spigot verbunden " + ch.id().asLongText());
                            // Unsere Custom-Codecs
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

            ChannelFuture f = b.bind(port).sync();
            this.channel = f.channel();
            System.out.println("[Proxy] Gestartet auf Port " + port);
            f.channel().closeFuture().addListener(future -> {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            });
        } catch (Exception e) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            throw e;
        }
    }



    @Override public Channel getChannel()
    {
        return channel;
    }
}