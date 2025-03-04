package org.cc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

public class OtherRecvServer implements Runnable {


    private int port;
    private EventLoopGroup group;
    private ServerBootstrap sb;

    // 例如在类中定义这些常量
    private static final int PORT = 9999;
    private static final int SO_RCVBUF_SIZE = 1024 * 1024 * 10; // 10MB
    private static final int BACKLOG_SIZE = 102400;
    private static final int IDLE_TIMEOUT = 180; // seconds


    public OtherRecvServer(int port) {
        this.port = port;

        System.out.println("打开接收第三方数据端口 ： " + port);
    }


    @Override
    public void run() {
        try {
            this.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @throws Exception 接收 转发的数据
     */

    public void start() throws Exception {
        group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
        try {
            sb = new ServerBootstrap();
            sb.group(group)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(this.port)
                    .option(ChannelOption.SO_RCVBUF, SO_RCVBUF_SIZE)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(SO_RCVBUF_SIZE))
                    .option(ChannelOption.SO_BACKLOG, BACKLOG_SIZE)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 180 * 1000)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ByteBuf delimiters = Unpooled.copiedBuffer("\r\n".getBytes());
                            ch.pipeline().addLast(new IdleStateHandler(IDLE_TIMEOUT, 0, 0));
                            ch.pipeline().addLast(new ChannelDuplexHandler() {
                                @Override
                                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                    if (evt instanceof IdleStateEvent) {
                                        ctx.close();
                                    }
                                }
                            });
                            ch.pipeline().addLast(new DelimiterBasedFrameDecoder(10240000, delimiters));
                            ch.pipeline().addLast(new StringDecoder());
                            ch.pipeline().addLast(new OtherHandle());
                        }
                    });

            System.out.println("Starting server on port: " + this.port);
            ChannelFuture cf = sb.bind().sync();
            cf.channel().closeFuture().sync();
        } catch (Exception e) {
            System.err.println("Server startup failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (group != null) {
                group.shutdownGracefully().sync();
            }
        }
    }


}
