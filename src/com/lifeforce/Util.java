package com.lifeforce;

import java.sql.Timestamp;
import java.util.UUID;

public final class Util {

	public static Timestamp getCurrentTimeStamp() {
		java.util.Date date = new java.util.Date();
		return new Timestamp(date.getTime());
	}

	public static String generateUUID() {
		return UUID.randomUUID().toString();
	}

}
