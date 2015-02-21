package com.lifeforce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Handles database operations like insert, update, delete and fetch.
 */
public class DbHandler implements Subject {
	
	private Connection conn = null;
	private static final String USER_NAME	= "admin";
	private static final String PASSWORD	= "admin";
	private static final String DB_NAME		= "275";

	/**
	 * Opens database
	 */
	private void openDatabase() {
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + DB_NAME, USER_NAME, PASSWORD);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Initializes database tables
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void initializeDb() throws ClassNotFoundException, SQLException {
		Connection c = null;
		Statement st = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + DB_NAME, USER_NAME, PASSWORD);
			String createTableQuery = "CREATE TABLE IF NOT EXISTS " + MyImage.IMAGES_TABLE +
			"(" +
			    MyImage.ID_COLUMN +					" serial primary key, " +
			    MyImage.NAME_COLUMN +				" varchar(50) not null, " +
			    MyImage.UUID_COLUMN +				" VARCHAR(50) not null, " +
			    MyImage.CREATION_TIME_COLUMN +		" timestamp, " +
			    MyImage.LAST_MODIFIED_TIME_COLUMN +	" timestamp, " +
			    MyImage.DATA_COLUMN	+				" bytea" +
			")";
			st = c.createStatement();
			st.execute(createTableQuery);
		} catch (ClassNotFoundException e) {
			throw e;
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if(st != null)
					st.close();
			} catch(SQLException e) {}
			try {
				if(c != null)
					c.close();
			} catch(SQLException e) {}
		}
	}
	
	/**
	 * Inserts data into database
	 * @param dbModel Object which is to be inserted
	 * @return Rows affected
	 * @throws SQLException
	 */
	public int insert(DbModel dbModel) throws SQLException {
		if(dbModel == null)
			return 0;
		int rowsAffected = 0;
		PreparedStatement ps = null;
		try {
			openDatabase();
			ps = dbModel.createInsertStatement(conn);
			rowsAffected = ps.executeUpdate();
			notifyObservers();
		} finally {
			if(ps != null) {
				ps.close();
			}
			closeDatabase();
		}
		return rowsAffected;
	}
	
	/**
	 * Updates provided record
	 * @param dbModel Object which is to be updated
	 * @return Rows affected
	 * @throws SQLException
	 */
	public int update(DbModel dbModel) throws SQLException {
		if(dbModel == null)
			return 0;
		int rowsAffected = 0;
		PreparedStatement ps = null;
		try {
			openDatabase();
			ps = dbModel.createUpdateStatement(conn);
			rowsAffected = ps.executeUpdate();
		} finally {
			if(ps != null) {
				ps.close();
			}
			closeDatabase();
		}
		return rowsAffected;
	}
	
	/**
	 * Fetches data from database
	 * @param dbModel
	 * @param responseModelType
	 * @return
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public DbModel get(DbModel dbModel, Class<? extends DbModel> responseModelType) throws SQLException, InstantiationException, IllegalAccessException  {
		if(dbModel == null || responseModelType == null)
			return null;
		
		DbModel responseModel = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			openDatabase();
			ps = dbModel.createSelectStatement(conn);
			rs = ps.executeQuery();
			
			if(rs != null && rs.next()) {
				responseModel = (DbModel) responseModelType.newInstance();
				responseModel.populateObjectFromResultSet(rs);
			}
		} finally {
			try {
				if(ps != null)
					ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			closeDatabase();
		}
		return responseModel;
	}
	
	/**
	 * Deletes record from database
	 * @param dbModel Record details which are to be deleted
	 * @return Number of rows affected
	 * @throws SQLException
	 */
	public int delete(DbModel dbModel) throws SQLException {
		if(dbModel == null)
			return 0;
		
		int rowsAffected = 0;
		openDatabase();
		PreparedStatement ps = null;
		try {
			ps = dbModel.createDeleteStatement(conn);
			rowsAffected = ps.executeUpdate();
		} finally {
			try {
				if(ps != null)
					ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			closeDatabase();
		}
		return rowsAffected;
	}
	
	/**
	 * Closes database connection
	 */
	private void closeDatabase() {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void attachObserver(Observer observer) {
		observers.add(observer);
	}

	@Override
	public void detachObserver(Observer observer) {
		observers.remove(observer);
	}

	@Override
	public void notifyObservers() {
		for(Observer observer : observers)
			observer.notify(this);
	}
}
