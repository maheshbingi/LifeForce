package poke.resources;

import com.google.protobuf.ByteString;
import com.lifeforce.MyImage;
import com.lifeforce.ResponseMessages;

import eye.Comm.PhotoHeader.ResponseFlag;
import eye.Comm.Request;

/**
 * Handles image fetch job
 */
public class ReadJobResource extends JobResource {

	@Override
	public Request process(Request request) {
		doInitialization(request);
		try {
			MyImage image = new MyImage(imageRequest.getName(), imageRequest.getUuid());
			MyImage responseImage = (MyImage) dbHandler.get(image, MyImage.class);

			if (responseImage != null && responseImage.getName() != null
					&& responseImage.getName().length() > 0) {
				responsePhotoPayload.setName(responseImage.getName());
				responsePhotoPayload.setUuid(responseImage.getUuid());
				responsePhotoPayload.setData(ByteString.copyFrom(responseImage.getData()));
				responsePhotoPayload.setResponseMessage(ResponseMessages.IMAGE_FETCHED_SUCCESSFULLY);
				
				responsePhotoHeader.setResponseFlag(ResponseFlag.success);
			} else {
				responsePhotoHeader.setResponseFlag(ResponseFlag.failure);
				responsePhotoPayload.setResponseMessage(ResponseMessages.IMAGE_NOT_FOUND);
			}
		} catch (Exception e) {
			e.printStackTrace();
			responsePhotoHeader.setResponseFlag(ResponseFlag.failure);
			responsePhotoPayload.setResponseMessage(ResponseMessages.SERVER_ERROR + e.getMessage());
		}
		return buildResponse();
	}
	
}
