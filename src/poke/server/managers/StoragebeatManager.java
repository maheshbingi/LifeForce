package poke.server.managers;

import io.netty.channel.Channel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import poke.server.conf.ServerConf;

import com.lifeforce.Observer;
import com.lifeforce.Subject;

import eye.Comm.Management;
import eye.Comm.MgmtHeader;
import eye.Comm.Storagebeat;


/**
 * This class helps in taking decision while sharding is done
 * A flag called isStorageNode is maintained across all nodes in the network
 * at a given time only one node's flag is set to true. Intially it takes value
 * from the configuration file and this care must be taken manually. After that 
 * the network manages the status of this flag. Observer pattern has been
 * implemented while maintaining its status. Db handler is the subject so if any 
 * changes are made in the database it will notify it's observer.
 * 
 */
public class StoragebeatManager implements Observer {

	protected static AtomicReference<StoragebeatManager> instance = new AtomicReference<StoragebeatManager>();
	private static ServerConf conf;
	
	public static StoragebeatManager initManager(ServerConf conf) {
		StoragebeatManager.conf = conf;
		instance.compareAndSet(null, new StoragebeatManager());
		return instance.get();
	}

	public static StoragebeatManager getInstance() {
		return instance.get();
	}
	
	protected StoragebeatManager() {
	}
	
	public void processRequest(Management mgmt) {
		
		JobManager.getInstance().setStorageNode(true);
	}
	
	
	private Management generateSB() {
		Storagebeat.Builder h = Storagebeat.newBuilder();
		h.setTimeRef(System.currentTimeMillis());
		
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(StoragebeatManager.conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());

		Management.Builder b = Management.newBuilder();
		b.setHeader(mhb.build());
		b.setStoragebeat(h.build());
		
		return b.build();
	}

	@Override
	public void notify(Subject subject) {
		
		// send StorageBeat to next node
		ConcurrentHashMap<Channel, HeartbeatData> aa = HeartbeatManager.getInstance().getOutgoingHB();
		Channel ch = aa.keySet().iterator().next();
		ch.writeAndFlush(generateSB());
		
	}
}
