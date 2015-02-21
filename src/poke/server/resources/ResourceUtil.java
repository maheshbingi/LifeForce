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

import java.util.List;

import com.lifeforce.ResponseMessages;
import com.lifeforce.storage.DbConfigurations;

import poke.server.conf.ServerConf;
import eye.Comm.Header;
import eye.Comm.Header.Routing;
import eye.Comm.PhotoHeader.ResponseFlag;
import eye.Comm.Payload;
import eye.Comm.PhotoHeader;
import eye.Comm.PhotoPayload;
import eye.Comm.PokeStatus;
import eye.Comm.Request;
import eye.Comm.RoutingPath;

public class ResourceUtil {

	/**
	 * Build a forwarding request message. Note this will return null if the
	 * server has already seen the request.
	 * 
	 * @param req
	 *            The request to forward
	 * @param cfg
	 *            The server's configuration
	 * @return The request with this server added to the routing path or null
	 */
	public static Request buildForwardMessage(Request req, ServerConf cfg) {

		List<RoutingPath> paths = req.getHeader().getPathList();
		if (paths != null) {
			// if this server has already seen this message return null
			for (RoutingPath rp : paths) {
				if (cfg.getNodeId() == rp.getNodeId())
					return null;
			}
		}

		Request.Builder bldr = Request.newBuilder(req);
		Header.Builder hbldr = bldr.getHeaderBuilder();
		RoutingPath.Builder rpb = RoutingPath.newBuilder();
		rpb.setNodeId(cfg.getNodeId());
		rpb.setTime(System.currentTimeMillis());
		hbldr.addPath(rpb.build());

		return bldr.build();
	}

	/**
	 * build the response header from a request
	 * 
	 * @param reqHeader
	 * @param status
	 * @param statusMsg
	 * @return
	 */
	public static Header buildHeaderFrom(Header reqHeader, PokeStatus status, String statusMsg) {
		return buildHeader(reqHeader.getRoutingId(), status, statusMsg, reqHeader.getOriginator(), reqHeader.getTag());
	}

	public static Header buildHeader(Routing path, PokeStatus status, String msg, int fromNode, String tag) {
		Header.Builder bldr = Header.newBuilder();
		bldr.setOriginator(fromNode);
		bldr.setRoutingId(path);
		bldr.setTag(tag);
		bldr.setReplyCode(status);

		if (msg != null)
			bldr.setReplyMsg(msg);

		bldr.setTime(System.currentTimeMillis());

		return bldr.build();
	}

	public static Request buildError(Header reqHeader, PokeStatus status, String statusMsg) {
		Request.Builder errorResponse = Request.newBuilder();
		Header.Builder errorHeader = Header.newBuilder();
		Payload.Builder errorPayload = Payload.newBuilder();
		PhotoHeader.Builder errorPhotoHeader = PhotoHeader.newBuilder();
		PhotoPayload.Builder errorPhotoPayload = PhotoPayload.newBuilder();
		errorPhotoHeader.setResponseFlag(ResponseFlag.failure);
		errorPhotoPayload.setResponseMessage(ResponseMessages.SERVER_ERROR);
		
		errorHeader.setRoutingId(reqHeader.getRoutingId());
		errorHeader.setOriginator(reqHeader.getOriginator());
		errorHeader.setToNode(0);
		errorHeader.setOriginator(0);
		
		errorHeader.setPhotoHeader(errorPhotoHeader);
		errorPayload.setPhotoPayload(errorPhotoPayload);
		
		errorResponse.setHeader(errorHeader);
		errorResponse.setBody(errorPayload);

		return errorResponse.build();
	}
	
	/**
	 * Build request object which can be forwarded to next cluster
	 * @param request
	 * @return
	 */
	public static Request buildForwardToClusterRequest(Request request) {
		Request.Builder forwardRequest = Request.newBuilder();
		Header.Builder forwardHeader = Header.newBuilder();
		Payload.Builder forwardPayload = Payload.newBuilder();
		
		PhotoHeader.Builder forwardPhotoHeader = PhotoHeader.newBuilder();
		forwardPhotoHeader.setRequestType(request.getHeader().getPhotoHeader().getRequestType());
		
		// If our cluster is receiving request first then add entry node as our cluster id
		if (!request.getHeader().getPhotoHeader().hasEntryNode()) {
			forwardPhotoHeader.setEntryNode(String.valueOf(DbConfigurations.getClusterId()));

		} else {
			// append your cluster id in the entry node so
			// you can know you have seen this request
			String visitedNode = request.getHeader().getPhotoHeader().getEntryNode();
			visitedNode += "," + String.valueOf(DbConfigurations.getClusterId());
			forwardPhotoHeader.setEntryNode(visitedNode);
		}
		
		forwardHeader.setRoutingId(request.getHeader().getRoutingId());
		forwardHeader.setOriginator(request.getHeader().getOriginator());
		forwardHeader.setToNode(request.getHeader().getToNode());
		
		forwardHeader.setPhotoHeader(forwardPhotoHeader);
		forwardPayload.setPhotoPayload(request.getBody().getPhotoPayload());
		
		forwardRequest.setHeader(forwardHeader);
		forwardRequest.setBody(forwardPayload);

		return forwardRequest.build();
	}
}
