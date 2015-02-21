package poke.resources;

import java.sql.SQLException;

import com.lifeforce.MyImage;
import com.lifeforce.ResponseMessages;

import eye.Comm.PhotoHeader.ResponseFlag;
import eye.Comm.Request;

/**
 * Handles image delete job
 */
public class DeleteJobResource extends JobResource {

	@Override
	public Request process(Request request) {
		doInitialization(request);
		try {
			MyImage image = new MyImage(imageRequest.getName(), imageRequest.getUuid());
			
			int rowsDeleted = dbHandler.delete(image);
			responsePhotoHeader.setResponseFlag(rowsDeleted > 0 ? ResponseFlag.success : ResponseFlag.failure);
			responsePhotoPayload.setResponseMessage(rowsDeleted > 0 ? ResponseMessages.IMAGE_DELETED_SUCCESSFULLY : ResponseMessages.IMAGE_NOT_FOUND);
			
		} catch (SQLException e) {
			e.printStackTrace();
			responsePhotoHeader.setResponseFlag(ResponseFlag.failure);
			responsePhotoPayload.setResponseMessage(ResponseMessages.SERVER_ERROR + e.getMessage());
		}
		return buildResponse();
	}

}
