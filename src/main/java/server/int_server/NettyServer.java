package server.int_server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;


public class NettyServer {

	private int port;

	public NettyServer(int port) {
		this.port = port;
	}

	public void run() throws Exception {
		EventLoopGroup bossGroup = new NioEventLoopGroup(); 
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap(); 
			b.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.childHandler(new ChannelInitializer<SocketChannel>() { 
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline()
					.addLast("decoder", new HttpRequestDecoder(4096, 8192, 8192, false))
					.addLast("aggregator", new HttpObjectAggregator(100 * 1024 * 1024))
					.addLast("handler",new ServerHandler());
				}
			})
			.option(ChannelOption.SO_BACKLOG, 128)         
			.childOption(ChannelOption.SO_KEEPALIVE, true); 

			System.out.println("Server Initiated Successfully...");
			System.out.println("Waiting for Incoming data...");
			
			// Bind and start to accept incoming connections.
			ChannelFuture f = b.bind(port).sync(); 

			// Wait until the server socket is closed.
			// In this example, this does not happen, but you can do that to gracefully
			// shut down your server.

			f.channel().closeFuture().sync();
		} finally { 
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

	public static void main(String[] args) throws Exception {
		int port;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		} else {
			port = 8087;
		}
		new NettyServer(port).run();
	}
}