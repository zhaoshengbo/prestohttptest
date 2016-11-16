package org.test.presto.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PrestoJdbcTest {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		new PrestoJdbcTest().test();
	}

	private void test() throws SQLException, ClassNotFoundException {
		Class.forName("com.facebook.presto.jdbc.PrestoDriver");
		Connection conn = null;
		Statement stat = null;
		try {
			conn = this.getConnection();
			stat = conn.createStatement();
			stat.setFetchSize(100);
			ResultSet rs = stat.executeQuery("select * from sqlserver.dbo.t_b_city limit 1000");
			this.printResultSet(rs);
		} finally {
			if (stat != null) {
				stat.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}

	private void printResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		int colCount = md.getColumnCount();
		for (int i = 1; i <= colCount; i++) {
			System.out.print(md.getColumnName(i) + "\t");
		}
		System.out.println("");
		while (rs.next()) {
			for (int i = 1; i < colCount; i++) {
				System.out.print(this.toString(rs.getObject(i)) + "\t");
			}
			System.out.println("");
		}
	}

	private String toString(Object obj) {
		if (obj == null) {
			return "";
		}
		return obj.toString();
	}

	private Connection getConnection() throws SQLException {
		Properties prop = new Properties();
		prop.put("user", "presto");
		return DriverManager.getConnection("jdbc:presto://10.104.102.184:8888/hive/bi_ods", prop);
	}

}
