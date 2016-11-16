package org.test.presto.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class SqlServerTest {

	public static void main(String[] args) {
		try {
			new SqlServerTest().test();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void test() throws ClassNotFoundException, SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String url = "jdbc:sqlserver://10.104.20.40:1433;DatabaseName=BI_COMMON";
		Properties prop = new Properties();
		prop.setProperty("user", "bi_ucar");
		prop.setProperty("password", "BI_ucar2015");
		Connection conn = DriverManager.getConnection(url, prop);
		DatabaseMetaData metadata = conn.getMetaData();
		ResultSet rs = metadata.getSchemas();

		String escape = metadata.getSearchStringEscape();
		System.out.println(conn.getCatalog());
		// ResultSet rs = metadata.getTables(conn.getCatalog(), "dbo", null, new
		// String[] { "TABLE", "VIEW" });
		int colCount = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= colCount; i++) {
			System.out.println(rs.getMetaData().getColumnName(i));
		}
		while (rs.next()) {
			System.out.println(rs.getString("TABLE_SCHEM"));
			// System.out.println(rs.getString("TABLE_NAME"));
		}
	}

}
