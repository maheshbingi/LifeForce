package com.lifeforce;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Provides contract for basic implementation of database operations
 */
public interface DbModel {
	public PreparedStatement createInsertStatement(Connection con) throws SQLException;
	public PreparedStatement createUpdateStatement(Connection con) throws SQLException;
	public PreparedStatement createSelectStatement(Connection con) throws SQLException;
	public PreparedStatement createDeleteStatement(Connection con) throws SQLException;
	public void populateObjectFromResultSet(ResultSet rs) throws SQLException;
}
