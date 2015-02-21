/*
 * copyright 2014, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.managers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.monitor.MonitorHandler;
import poke.monitor.MonitorInitializer;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import eye.Comm.Management;
import eye.Comm.MgmtHeader;
import eye.Comm.Network;
import eye.Comm.Network.NetworkAction;
import eye.Comm.NetworkRecoveryData;

/**
 * The network manager contains the node's view of the network. This view is
 * created through messages sent by other nodes to this node. For every
 * connection created, this manager creates a map.
 * 
 * @author gash
 * 
 */
public class NetworkManager {
	protected static Logger logger = LoggerFactory.getLogger("network");
	protected static AtomicReference<NetworkManager> instance = new AtomicReference<NetworkManager>();

	private static ServerConf conf;
	private String nextNodeIP;
	private int nextNodePort;
	private int nextNodeId;

	public String getNextNodeIP() {
		return nextNodeIP;
	}

	public void setNextNodeIP(String nextNodeIP) {
		this.nextNodeIP = nextNodeIP;
	}

	public int getNextNodePort() {
		return nextNodePort;
	}

	public void setNextNodePort(int nextNodePort) {
		this.nextNodePort = nextNodePort;
	}

	/** @brief the number of votes this server can cast */
	private int votes = 1;

	public static NetworkManager initManager(ServerConf conf) {
		NetworkManager.conf = conf;
		instance.compareAndSet(null, new NetworkManager());
		return instance.get();
	}

	public static NetworkManager getInstance() {
		// TODO throw exception if not initialized!
		return instance.get();
	}

	/**
	 * initialize the manager for this server
	 * 
	 */
	protected NetworkManager() {

	}

	/**
	 * @param args
	 */
	public void processRequest(Management mgmt, Channel channel) {
		Network req = mgmt.getGraph();
		if (req == null || channel == null)
			return;

		logger.info("Network: node '" + req.getFromNodeId() + "' sent a " + req.getAction());

		/**
		 * Outgoing: when a node joins to another node, the connection is
		 * monitored to relay to the requester that the node (this) is active -
		 * send a heartbeatMgr
		 */
		if (req.getAction().getNumber() == NetworkAction.NODEJOIN_VALUE) {
			if (channel.isOpen()) {
				// can i cast socka?
				SocketAddress socka = channel.localAddress();
				if (socka != null) {
					// this node will send messages to the requesting client
					InetSocketAddress isa = (InetSocketAddress) socka;
					logger.info("NODEJOIN: " + isa.getHostName() + ", " + isa.getPort());
					
					setNextNodeIP(mgmt.getHeader().getIpAddress());
					setNextNodePort(mgmt.getHeader().getPort());
					setNextNodeId(mgmt.getGraph().getFromNodeId());
					
					ConcurrentHashMap<Channel, HeartbeatData> outgoingHb = HeartbeatManager.getInstance().getOutgoingHB();
					if(!outgoingHb.isEmpty()){ // edge already present then disconnect with this node and accept the incoming node in the network
						int connectToNodeId = req.getFromNodeId(); // newly added node which will become our toNodeId
						int leaveFromNodeId = conf.getNodeId(); // self id from which the next node should disconnect from
						Entry<Channel, HeartbeatData> toNodeInfo = outgoingHb.entrySet().iterator().next();
						Channel ch = toNodeInfo.getKey();
						HeartbeatManager.getInstance().removeFromOutgoingHB(toNodeInfo.getKey());
						Management leaveMsg = createLeaveMsg(leaveFromNodeId, connectToNodeId);
						ch.writeAndFlush(leaveMsg);
					}
					
					HeartbeatManager.getInstance().addOutgoingChannel(req.getFromNodeId(), isa.getHostName(),
							isa.getPort(), channel, socka);
				}
			} else
				logger.warn(req.getFromNodeId() + " not writable");
		} else if (req.getAction().getNumber() == NetworkAction.NODEDEAD_VALUE) {
			
			ConcurrentHashMap<Channel, HeartbeatData> outgoingHB = HeartbeatManager.getInstance().getOutgoingHB();
			
			// check Next NODE is a DEAD NODE
			if(outgoingHB.keySet().isEmpty() || (outgoingHB.values().iterator().next().getNodeId() == req.getFromNodeId())) {
				// check isStorage flag in network recovery data message is FALSE
				// then SET self JobManager-> isStorage to TRUE
				if(req.getNetworkRecoveryData().getIsStorageNode() == false){
					JobManager.getInstance().setStorageNode(true);
				}
				NetworkRecoveryData obj = req.getNetworkRecoveryData();
				Channel ch = createChannel(obj.getHost(),obj.getMgmtport());

				// create a connect me msg for connecting directly with the node whose sender node was failed
				Management msg = createConnectMeMsg(req.getFromNodeId()); 
				ch.writeAndFlush(msg);
			}else{// else if next node is not a dead node then forward the request to next node
				// check isStorage flag in network recovery data message is FALSE
				// and self JobManager-> isStorage to TRUE 
				// THEN set (network recovery data -> isStorage) to TRUE and forward request
				
				if((req.getNetworkRecoveryData().getIsStorageNode()) == false && (JobManager.getInstance().isStorageNode() == true)){
					mgmt = updateStorageFlag(req);
				}
			
				Channel ch = outgoingHB.keySet().iterator().next();
				ch.writeAndFlush(mgmt);
			}
		} else if (req.getAction().getNumber() == NetworkAction.NODELEAVE_VALUE) {
			// node removing itself from the network (gracefully)

			for (NodeDesc nn : conf.getAdjacent().getAdjacentNodes().values()) {
				//it will check if it was a next it's adjecent node or not if yes connect else don't 
				//here we have one assumption that the node that comes up will be a previous node from the node
				//This case will fail when there are more than one sequential failure in ring.
				//assumption : there won't be multiple failure in a ring.
				
				if(req.getToNodeId() == nn.getNodeId()){			//create new edge with this node
					channel.closeFuture();
					HeartbeatManager.getInstance().removeAdjacentNode(req.getFromNodeId());
				
					HeartbeatData node = new HeartbeatData(nn.getNodeId(), nn.getHost(), nn.getPort(), nn.getMgmtPort());
					HeartbeatPusher.getInstance().connectToThisNode(conf.getNodeId(), node);
				}else{
					System.out.println("Unknown node trying to connect");
					
				}
			}
			
		} else if (req.getAction().getNumber() == NetworkAction.ANNOUNCE_VALUE) {
			// nodes sending their info in response to a create map
		} else if (req.getAction().getNumber() == NetworkAction.CREATEMAP_VALUE) {
			// request to create a network topology map
		}else if(req.getAction().getNumber() == NetworkAction.CONNECTME_VALUE){ 
			//a new channel is created for sending the information of next Node and 
			//with Connectme msg. This node will send NODEJOIN msg to sender node.
			//current channel is closed and new Channel will be created in NODEJOIN process
			
			channel.disconnect();
			channel.close();
			NetworkRecoveryData hbDataMsg = req.getNetworkRecoveryData();
			HeartbeatData hbData = new HeartbeatData(hbDataMsg.getNodeId(),hbDataMsg.getHost(),hbDataMsg.getPort(),hbDataMsg.getMgmtport());
			logger.info("Received CONNECTME message from :" + hbData.getNodeId());
			HeartbeatPusher.getInstance().connectToThisNode(conf.getNodeId(), hbData);
			
		}

		// may want to reply to exchange information
	}

	
	
	/**
	 * This method creates a connect me message which is basically used for 
	 * telling other node to create a edge with sender node
	 *  
	 * CONNECTME is a action type created for this purpose
	 * 
	 * The format followed by the network message is
	 * 
	 * | fromNodeId | toNodeId   | NetworkRecoveryData | NetworkAction |
	 * | Disconnect | activeNode | ActiveNodeInfo      | CONNECTME     |
	 * Disconnect : Disconnect from the dead node
	 * ActiveNode : Connect to this node whose information is given in ActiveNodeInfo object
	 * 
	 * @param toNodeId
	 * @return
	 */
	private Management createConnectMeMsg(int toNodeId) {
				
		Network.Builder n = Network.newBuilder();
		
		NetworkRecoveryData.Builder hb = NetworkRecoveryData.newBuilder();
		HeartbeatData hbData = getSelfInfo();
		hb.setNodeId(hbData.getNodeId());
		hb.setHost(hbData.getHost());
		hb.setPort(hbData.getPort());
		hb.setMgmtport(hbData.getMgmtport());
		hb.setIsStorageNode(JobManager.getInstance().isStorageNode());

		// 'N' allows us to track the connection restarts and to provide
		// uniqueness
		n.setFromNodeId(conf.getNodeId());
		n.setToNodeId(toNodeId);
		n.setNetworkRecoveryData(hb);
		n.setAction(NetworkAction.CONNECTME);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());

		Management.Builder m = Management.newBuilder();
		m.setHeader(mhb.build());
		m.setGraph(n.build());
		return m.build();		
	}
	
	
	
	
	/**
	 * leaveFromNodeID : disconnect from this node
	 * connectToNodeId : connect to this active node
	 * 
	 * @param leaveFromNodeId
	 * @param connectToNodeId
	 * @return
	 */
	private Management createLeaveMsg(int leaveFromNodeId,  int connectToNodeId) {
		
		Network.Builder n = Network.newBuilder();
		
		// 'N' allows us to track the connection restarts and to provide
		// uniqueness
		n.setFromNodeId(leaveFromNodeId);
		n.setToNodeId(connectToNodeId);
		n.setAction(NetworkAction.NODELEAVE);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());

		Management.Builder m = Management.newBuilder();
		m.setHeader(mhb.build());
		m.setGraph(n.build());
		return m.build();		
	}


	/**
	 * Method which provides all information about this node
	 * 
	 * @return
	 */
	public HeartbeatData getSelfInfo() {
		HeartbeatData hbData = null;
		hbData = new HeartbeatData(conf.getNodeId(), conf.getHost(), conf.getPort(),
				conf.getMgmtPort());
		return hbData;
	}
	
	
	
	/**
	 * For now this channel is created for temporary communication while sending CONNECTME 
	 * message. Once the other node gets this message it will close the channel and will 
	 * send a NODEJOIN.
	 *  
	 * @param host
	 * @param port
	 * @return
	 */
	private Channel createChannel(String host, int port){
		ChannelFuture channel = null;
		try {
			MonitorHandler handler = new MonitorHandler();
			MonitorInitializer mi = new MonitorInitializer(handler, false);

			Bootstrap b = new Bootstrap();
			b.group(new NioEventLoopGroup()).channel(NioSocketChannel.class).handler(mi);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			// Make the connection attempt.
			channel = b.connect(host, port).syncUninterruptibly();
			channel.awaitUninterruptibly(5000l);
			
		} catch (Exception ex) {
			logger.debug("failed to initialize the heartbeat connection");
			// logger.error("failed to initialize the heartbeat connection",
			// ex);
		}
		return channel.channel();
	}
	
	/**
	 * updates the isStorageNode value with it's own value
	 * 
	 * @param oldNRMsg
	 * @return
	 */
	private Management updateStorageFlag(Network oldNRMsg)
	{
		Network.Builder n = Network.newBuilder();
		
		NetworkRecoveryData.Builder nrData = oldNRMsg.getNetworkRecoveryData().newBuilder();
		
		NetworkRecoveryData oldNRData = oldNRMsg.getNetworkRecoveryData();
		
		nrData.setNodeId(oldNRData.getNodeId());
		nrData.setHost(oldNRData.getHost());
		nrData.setPort(oldNRData.getPort());
		nrData.setMgmtport(oldNRData.getMgmtport());
		
		nrData.setIsStorageNode(JobManager.getInstance().isStorageNode());
		
		n.setFromNodeId(oldNRMsg.getFromNodeId());
		n.setToNodeId(oldNRMsg.getToNodeId());
		n.setNetworkRecoveryData(nrData);
		n.setAction(oldNRMsg.getAction());

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(oldNRMsg.getFromNodeId());
		mhb.setTime(System.currentTimeMillis());

		Management.Builder m = Management.newBuilder();
		m.setHeader(mhb.build());
		m.setGraph(n.build());
		return m.build();	
		
	}

	public int getNextNodeId() {
		return nextNodeId;
	}

	public void setNextNodeId(int nextNodeId) {
		this.nextNodeId = nextNodeId;
	}

}
