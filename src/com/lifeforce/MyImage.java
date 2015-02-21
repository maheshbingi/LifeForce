package com.lifeforce;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Implementation of image
 */
public class MyImage implements DbModel {
	
	public static final String IMAGES_TABLE		= "images";
	public static final String ID_COLUMN		= "id";
	public static final String NAME_COLUMN		= "name";
	public static final String UUID_COLUMN		= "uuid";
	public static final String DATA_COLUMN		= "data";
	public static final String CREATION_TIME_COLUMN			= "created_time";
	public static final String LAST_MODIFIED_TIME_COLUMN	= "last_modified_time";
	
	private int id;
	private String name;
	private String uuid;
	private long size;
	private Timestamp createdTime;
	private Timestamp lastModifiedTime;
	private byte[] data;
	
	public MyImage() {}
	
	public MyImage(String name, String uuid) {
		this(name, uuid, 0, null, null, null);
	}
	
	public MyImage(String name, String uuid, long size, Timestamp createdTime, Timestamp lastModifiedTime, byte[] data) {
		setName(name);
		setUuid(uuid);
		setSize(size);
		setCreatedTime(createdTime);
		setLastModifiedTime(lastModifiedTime);
		setData(data);
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	public Timestamp getCreatedTime() {
		return createdTime;
	}
	public void setCreatedTime(Timestamp createdTime) {
		this.createdTime = createdTime;
	}
	public Timestamp getLastModifiedTime() {
		return lastModifiedTime;
	}
	public void setLastModifiedTime(Timestamp lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	
	@Override
	public String toString() {
		return name + "  " + uuid;
	}

	/**
	 * Provides implementation for populating object from result set 
	 */
	@Override
	public void populateObjectFromResultSet(ResultSet rs) throws SQLException {
		setName(rs.getString(NAME_COLUMN));
		setData(rs.getBytes(DATA_COLUMN));
		setUuid(rs.getString(UUID_COLUMN));
	}

	/**
	 * Implementation of creating statement to update image details
	 */
	@Override
	public PreparedStatement createUpdateStatement(Connection con) throws SQLException {
		String updateQuery = "UPDATE " + IMAGES_TABLE + " SET " + DATA_COLUMN + "= ?, " + LAST_MODIFIED_TIME_COLUMN + "= ?" +
				" WHERE " + UUID_COLUMN + " = ? ";
		PreparedStatement ps1 = con.prepareStatement(updateQuery);
		ps1.setObject(1, getData());
		ps1.setTimestamp(2, getLastModifiedTime());
		ps1.setString(3, getUuid());
		return ps1;
	}

	/**
	 * Implementation of creating statement to insert image details
	 */
	@Override
	public PreparedStatement createInsertStatement(Connection con) throws SQLException {
		String insertQuery = "INSERT INTO " + IMAGES_TABLE + " (" + NAME_COLUMN + ", " + UUID_COLUMN + ", " + 
				CREATION_TIME_COLUMN + ", " + LAST_MODIFIED_TIME_COLUMN + ", " + DATA_COLUMN + ")"
				+ "VALUES (?, ?, ?, ?, ?)";
		PreparedStatement ps = con.prepareStatement(insertQuery);
		ps.setString(1, getName());
		ps.setString(2, getUuid());
		ps.setTimestamp(3, getCreatedTime());
		ps.setTimestamp(4, getLastModifiedTime());
		ps.setObject(5, getData());
		return ps;
	}

	/**
	 * Implementation of creating statement to fetch image details
	 */
	@Override
	public PreparedStatement createSelectStatement(Connection con) throws SQLException {
		String selectQuery = "SELECT * FROM " + IMAGES_TABLE + " WHERE " + UUID_COLUMN + " = ?";
		PreparedStatement ps = con.prepareStatement(selectQuery);
		ps.setString(1, getUuid());
		return ps;
	}

	/**
	 * Implementation of creating statement to delete image details
	 */
	@Override
	public PreparedStatement createDeleteStatement(Connection con) throws SQLException {
		PreparedStatement ps = con.prepareStatement("DELETE FROM " + IMAGES_TABLE + " WHERE " + UUID_COLUMN + " = ?");
		ps.setString(1, getUuid());
		return ps;
	}
}
