package com.lifeforce;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import poke.server.managers.ElectionManager;
import poke.server.managers.NetworkManager;
import poke.server.queue.ChannelQueue;
import poke.server.resources.ResourceUtil;

import com.lifeforce.storage.ClusterDBServiceImplementation;
import com.lifeforce.storage.ClusterMapperManager;
import com.lifeforce.storage.ClusterMapperStorage;

import eye.Comm.PhotoHeader.ResponseFlag;

public class ChannelHandler extends ChannelInitializer<Channel> {
	private ChannelQueue channelQueue;

	public ChannelHandler(ChannelQueue channelQueue) {
		this.channelQueue = channelQueue;
	}
	@Override
	protected void initChannel(Channel arg0) throws Exception {
		ChannelPipeline pipeline = arg0.pipeline();
		pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4));
		pipeline.addLast("protobufDecoder", new ProtobufDecoder(eye.Comm.Request.getDefaultInstance()));
		pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
		pipeline.addLast("protobufEncoder", new ProtobufEncoder());
		
		pipeline.addLast(new SimpleChannelInboundHandler<eye.Comm.Request>() {
			protected void channelRead0(ChannelHandlerContext ctx, eye.Comm.Request reply) throws Exception {
				if(channelQueue != null) {
					
					// If inter-cluster request forwarding implementation is turned on then only forward request
					if(ClusterDBServiceImplementation.isInterClusterImplementation()) {
						
						try {
							// If image is not found in this cluster then forward it to next cluster
							if (reply.getHeader().getPhotoHeader().getResponseFlag() == ResponseFlag.failure
									&& null != ElectionManager.getInstance().whoIsTheLeader()
									&& ElectionManager.getInstance().whoIsTheLeader() == NetworkManager
											.getInstance().getSelfInfo().getNodeId()) {
		
								String visitedNodes = reply.getHeader().getPhotoHeader().getEntryNode();
								
								ClusterMapperStorage nextClusterMaster = ClusterMapperManager.getClusterDetails(visitedNodes);
								
								/*
								 *  If next cluster details are present then forward request,
								 *  else our's is the last cluster, so return it to caller
								 */
								if(nextClusterMaster != null) {
									String nextClusterIp = nextClusterMaster.getLeaderHostAddress();
									int nextClusterPort = nextClusterMaster.getPort();
									
									RemoteConnection remoteConnection = new RemoteConnection(nextClusterIp, nextClusterPort, channelQueue);
									remoteConnection.sendMessage(ResourceUtil.buildForwardToClusterRequest(reply));
								} else {
									channelQueue.enqueueResponse(reply, null);
								}
								
							} else {
								channelQueue.enqueueResponse(reply, null);
							}
						} catch (Exception e) {
							e.printStackTrace();
							channelQueue.enqueueResponse(reply, null);
						}
					} else {
						channelQueue.enqueueResponse(reply, null);
					}
					
				}
			}
		});
	}

	public ChannelQueue getChannelQueue() {
		return channelQueue;
	}

	public void setChannelQueue(ChannelQueue channelQueue) {
		this.channelQueue = channelQueue;
	}

}
