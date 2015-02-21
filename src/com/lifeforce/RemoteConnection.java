package com.lifeforce;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.queue.ChannelQueue;
import eye.Comm.Request;

/**
 * Handles connection with remote server
 */
public class RemoteConnection {
	protected static Logger logger = LoggerFactory.getLogger("server");
	private String host;
	private int port;
	private ChannelQueue channelQueue;
	private ChannelHandler handler;
	private ChannelFuture futureChannel;
	
	/** 
	 * Multiple channels can share single event loop group object, hence declaring static final.
	 * Is this approach correct?
	 */
	private static final NioEventLoopGroup group = new NioEventLoopGroup();
	
	public RemoteConnection(String host, int port, ChannelQueue channelQueue) {
		this.host = host;
		this.port = port;
		this.channelQueue = channelQueue;
		init();
	}
	
	private void init() {
		try {
			handler = new ChannelHandler(channelQueue);
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).handler(handler);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			// Make the connection attempt.
			futureChannel = b.connect(host, port).syncUninterruptibly();
		} catch (Exception ex) {
			logger.error("failed to initialize the client connection", ex);
		}
	}
	
	public void sendMessage(Request req) {
		Channel ch = futureChannel.channel();
		OutBoundThread thread = new OutBoundThread(ch, req);
		thread.run();
	}

	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}

	public ChannelQueue getChannelQueue() {
		return channelQueue;
	}

	public void setChannelQueue(ChannelQueue channelQueue) {
		this.channelQueue = channelQueue;
	}
	
	class OutBoundThread implements Runnable {
		private Channel ch;
		private Request request;
		
		public OutBoundThread(Channel ch, Request req) {
			this.ch = ch;
			this.request = req;
		}

		@Override
		public void run() {
			try {
				if(ch.isWritable())
					ch.writeAndFlush(request);
			} finally {
				request = null;
			}
		}
		
	}

}
