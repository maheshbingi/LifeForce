/*
 * copyright 2012, gash
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
package poke.server.resources;

import java.beans.Beans;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.resources.ForwardResource;
import poke.server.conf.ServerConf;
import poke.server.managers.JobManager;
import eye.Comm.Request;

/**
 * Resource factory provides how the server manages resource creation. We hide
 * the creation of resources to be able to change how instances are managed
 * (created) as different strategies will affect memory and thread isolation. A
 * couple of options are:
 * <p>
 * <ol>
 * <li>instance-per-request - best isolation, worst object reuse and control
 * <li>pool w/ dynamic growth - best object reuse, better isolation (drawback,
 * instances can be dirty), poor resource control
 * <li>fixed pool - favor resource control over throughput (in this case failure
 * due to no space must be handled)
 * </ol>
 * 
 * @author gash
 * 
 */
public class ResourceFactory {
	protected static Logger logger = LoggerFactory.getLogger("server");

	private static ServerConf cfg;
	private static AtomicReference<ResourceFactory> factory = new AtomicReference<ResourceFactory>();

	public static void initialize(ServerConf cfg) {
		try {
			ResourceFactory.cfg = cfg;
			factory.compareAndSet(null, new ResourceFactory());
		} catch (Exception e) {
			logger.error("failed to initialize ResourceFactory", e);
		}
	}

	public static ResourceFactory getInstance() {
		ResourceFactory rf = factory.get();
		if (rf == null)
			throw new RuntimeException("Server not intialized");

		return rf;
	}

	private ResourceFactory() {
	}

	/**
	 * Obtain a resource
	 * 
	 * @param route
	 * @return
	 */
	public Resource resourceInstance(Request request) {
		String UUID = request.getBody().getPhotoPayload().getUuid();
		switch(request.getHeader().getPhotoHeader().getRequestType()) {
		case write:
			if(JobManager.getInstance().isStorageNode() && 
					(UUID == null || UUID.trim().equals(""))) {
				
				// This is storage node
				return createJobResource("poke.resources.WriteJobResource");
				
			} else if(UUID != null && UUID.trim().length() > 0) {
				
				// Update request
				return createJobResource("poke.resources.UpdateJobResource");
				
			} else {
				
				// Message is not for me, Forward to next node
				return createForwardResource();
				
			}
		
		case read:
			return createJobResource("poke.resources.ReadJobResource");
		case delete:
			return createJobResource("poke.resources.DeleteJobResource");
		default:
			return null;
		}
	}
	
	/**
	 * Creates JobResource
	 * @param header
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private Resource createJobResource(String beanName) {
		Resource rsc = null;
		try {
			rsc = (Resource) Beans.instantiate(this.getClass().getClassLoader(), beanName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rsc;
	}
	
	/**
	 * Creates ForwardResource
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private Resource createForwardResource() {
		Resource rsc = null;
		try {
			rsc = (Resource) Beans.instantiate(this.getClass().getClassLoader(), cfg.getForwardingImplementation());
			((ForwardResource)rsc).setCfg(cfg);
		} catch (ClassNotFoundException e) {
			logger.error("Unable to create forwarding implementation");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rsc;
	}
}
