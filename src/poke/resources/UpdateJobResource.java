package poke.resources;

import com.lifeforce.MyImage;
import com.lifeforce.ResponseMessages;
import com.lifeforce.Util;

import eye.Comm.PhotoHeader.ResponseFlag;
import eye.Comm.Request;

/**
 * Handles image update job
 */
public class UpdateJobResource extends JobResource {

	@Override
	public Request process(Request request) {
		doInitialization(request);
		
		try {
			MyImage image = new MyImage(imageRequest.getName(), imageRequest.getUuid(), 0, 
					null, Util.getCurrentTimeStamp(), imageRequest.getData().toByteArray());
			
			// Update image to db
			int rowsAffected = dbHandler.update(image);
			
			if(rowsAffected > 0) {
				MyImage responseImage = (MyImage) dbHandler.get(image, MyImage.class);
				
				// Setting response payload fields
				responsePhotoPayload.setUuid(responseImage.getUuid());
				responsePhotoPayload.setName(responseImage.getName());
				responsePhotoPayload.setResponseMessage(ResponseMessages.IMAGE_UPDATED_SUCCESSFULLY);
				
				// Setting response header fields
				responsePhotoHeader.setResponseFlag(ResponseFlag.success);
			} else {
				responsePhotoHeader.setResponseFlag(ResponseFlag.failure);
				responsePhotoPayload.setResponseMessage(ResponseMessages.IMAGE_NOT_UPDATED);
			}
			
		} catch(Exception e) {
			e.printStackTrace();
			responsePhotoHeader.setResponseFlag(ResponseFlag.failure);
			responsePhotoPayload.setResponseMessage(ResponseMessages.SERVER_ERROR + e.getMessage());
		}
		return buildResponse();
	}

}
