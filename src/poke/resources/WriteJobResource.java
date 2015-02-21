package poke.resources;

import poke.server.managers.JobManager;
import poke.server.managers.StoragebeatManager;

import com.lifeforce.MyImage;
import com.lifeforce.ResponseMessages;
import com.lifeforce.Util;

import eye.Comm.PhotoHeader.ResponseFlag;
import eye.Comm.Request;

/**
 * Handles image insert job
 */
public class WriteJobResource extends JobResource {

	@Override
	public Request process(Request request) {
		doInitialization(request);
		
		// Attach observes to get notification when row is inserted into database
		dbHandler.attachObserver(JobManager.getInstance());
		dbHandler.attachObserver(StoragebeatManager.getInstance());
		
		try {
			MyImage image = new MyImage(imageRequest.getName(), Util.generateUUID(), imageRequest.getData().toByteArray().length, 
						Util.getCurrentTimeStamp(), Util.getCurrentTimeStamp(), imageRequest.getData().toByteArray());
			
			long imageSize = image.getData().length;
			
			// Validate image size
			if(imageSize < ALLOWED_FILE_SIZE) {
				// Writing image to db
				int rowsInserted = dbHandler.insert(image);
				
				if(rowsInserted > 0) {
					MyImage responseImage = (MyImage)dbHandler.get(image, MyImage.class);
					
					// Setting response payload fields
					responsePhotoPayload.setUuid(responseImage.getUuid());
					responsePhotoPayload.setName(responseImage.getName());
					responsePhotoPayload.setResponseMessage(ResponseMessages.IMAGE_INSERTED_SUCCESSFULLY);
					
					// Setting response header fields
					responsePhotoHeader.setResponseFlag(ResponseFlag.success);
				} else {
					responsePhotoHeader.setResponseFlag(ResponseFlag.failure);
					responsePhotoPayload.setResponseMessage(ResponseMessages.IMAGE_NOT_INSERTED);
				}
			} else {
				
				// File size is greater than allowed file size return failure message to caller
				responsePhotoHeader.setResponseFlag(ResponseFlag.failure);
				responsePhotoPayload.setResponseMessage("File size '" + ((float)imageSize/1024)  + "kb' exceeds permitted file size " + (ALLOWED_FILE_SIZE/1024) + "kb");
			}
		} catch (Exception e) {
			e.printStackTrace();
			responsePhotoHeader.setResponseFlag(ResponseFlag.failure);
			responsePhotoPayload.setResponseMessage(ResponseMessages.SERVER_ERROR + e.getMessage());
		} finally {
			dbHandler.detachObserver(JobManager.getInstance());
			dbHandler.detachObserver(StoragebeatManager.getInstance());
		}

		return buildResponse();
	}

}
