package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;

public class ReadSchemaMysql {
    public static void main(String[] args) {
        getTableInformation();
    }
    public static void getTableInformation(){
        // JDBC URL, username, and password
        String url = "jdbc:mysql://10.10.12.238:3306/payment_order";
        String username = "dbadmin";
        String password = "oracle_4U";

        // SQL query to get column information
        String tableName = "pmt_txn"; // Specify the table name here
        String query = "SELECT * FROM " + tableName + " LIMIT 0";

        String file = "mysql_table_" + tableName + ".json";

        JSONObject tableInfo = new JSONObject(); // JSON object to hold table information
        JSONArray columnsArray = new JSONArray(); // JSON array to hold column information


        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            // Get ResultSet metadata
            ResultSetMetaData metaData = rs.getMetaData();

            // Get column count
            int columnCount = metaData.getColumnCount();

            // Print column information
            System.out.println("Column Information for Table: " + tableName);
            for (int i = 1; i <= columnCount; i++) {
                JSONObject columnInfo = new JSONObject();
                String columnName = metaData.getColumnName(i);
                String columnType = metaData.getColumnTypeName(i);
                int columnSize = metaData.getColumnDisplaySize(i);
                boolean isNullable = (metaData.isNullable(i) == ResultSetMetaData.columnNullable);

                // Populate JSON object with column information
                columnInfo.put("name", columnName);
                columnInfo.put("type", columnType);
                columnInfo.put("size", columnSize);
                columnInfo.put("nullable", isNullable);

                columnsArray.put(columnInfo);

            }
            // Add the JSON array of columns to the table JSON object
            tableInfo.put(tableName, columnsArray);

            // Write JSON object to file
            try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
                writer.println(tableInfo.toString(4)); // Indented JSON with 4 spaces
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
