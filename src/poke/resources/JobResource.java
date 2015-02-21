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
package poke.resources;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.managers.ElectionManager;
import poke.server.managers.HeartbeatManager;
import poke.server.managers.NetworkManager;
import poke.server.resources.Resource;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.lifeforce.DbHandler;
import com.lifeforce.ResponseMessages;

import eye.Comm.Header;
import eye.Comm.Payload;
import eye.Comm.PhotoHeader;
import eye.Comm.PhotoHeader.ResponseFlag;
import eye.Comm.PhotoPayload;
import eye.Comm.Request;

/**
 * Provides common implementation for different jobs (like write, read, update and delete image)
 */
public abstract class JobResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("server");
	public static final String REPLY = "reply";
	public static final String FORWARD = "forward";
	
	/** Allowed image size */
	protected static final long ALLOWED_FILE_SIZE = 560 * 1024;
	
	protected Request.Builder responseRequest;
	protected Header.Builder responseHeader;
	protected Payload.Builder responsePayload;
	protected PhotoHeader.Builder responsePhotoHeader;
	protected PhotoPayload.Builder responsePhotoPayload;
	protected DbHandler dbHandler;
	protected PhotoPayload imageRequest;
	protected Request incomingRequest;
	
	/**
	 * Performs initialization of request parameters
	 * @param request
	 */
	protected void doInitialization(Request request) {
		incomingRequest = request;
		Payload payload = request.getBody();
		Header header = request.getHeader();
		
		// Main response request
		responseRequest = Request.newBuilder();
		responseHeader = Header.newBuilder();
		responsePayload = Payload.newBuilder();
		
		// Setting response header fields
		responseHeader.setRoutingId(header.getRoutingId());
		
		responseHeader.setReplyMsg(REPLY);
		
		responseHeader.setToNode(header.getToNode());
		responseHeader.setOriginator(header.getOriginator());
		
		// Photo response header and payload
		responsePhotoHeader = PhotoHeader.newBuilder();
		responsePhotoPayload = PhotoPayload.newBuilder();
		
		Map<FieldDescriptor, Object> map = payload.getAllFields();
		Iterator<FieldDescriptor> iterator = map.keySet().iterator();
		
		if(iterator != null && iterator.hasNext()) {
			FieldDescriptor field = iterator.next();
			imageRequest = (PhotoPayload) map.get(field);
		}
		dbHandler = new DbHandler();
	}
	
	/**
	 * Builds response object
	 * @return
	 */
	protected Request buildResponse() {
		try {
			int nextNodeId = HeartbeatManager.getInstance().getOutgoingHB().values().iterator().next().getNodeId();
			
			/*
			 * If this node does not have required data and next node is NOT the originator node, then
			 * only set reply field as 'forward'.
			 * Else set reply field as 'reply'
			 */
			if(responsePhotoHeader.getResponseFlag() == ResponseFlag.failure &&
					nextNodeId != responseHeader.getOriginator()) {
				responseHeader.setReplyMsg(FORWARD);
				responsePhotoHeader.setRequestType(incomingRequest.getHeader().getPhotoHeader().getRequestType());
				responsePhotoPayload.setName(imageRequest.getName());
				responsePhotoPayload.setUuid(imageRequest.getUuid());
				responsePhotoPayload.setData(imageRequest.getData());
			} else {
				responseHeader.setReplyMsg(REPLY);
			}
			
			// This is leader node, so set originator id as this node id
			if(ElectionManager.getInstance().whoIsTheLeader() != null) {
				responseHeader.setOriginator(NetworkManager.getInstance().getSelfInfo().getNodeId());
				logger.info("I am leader, adding originator id " + NetworkManager.getInstance().getSelfInfo().getNodeId() + " to request");
			}
		} catch (NoSuchElementException e) {
			logger.error("Ring might not be initialized. " + e.getMessage());
			responsePhotoPayload.setResponseMessage(ResponseMessages.SERVER_ERROR + "Network not iniatilized");
		}
				
		responseHeader.setPhotoHeader(responsePhotoHeader);
		responsePayload.setPhotoPayload(responsePhotoPayload);
		responseRequest.setHeader(responseHeader);
		responseRequest.setBody(responsePayload);
		return responseRequest.build();
	}

}