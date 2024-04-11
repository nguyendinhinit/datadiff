package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadSchemaOracle {
    public static void main(String[] args) throws IOException {
        getTableInformation();
    }
    public static void getTableInformation() throws IOException {
        String url = "jdbc:oracle:thin:@//10.10.12.21:1521/t24core";
        String username = "ggadmin";
        String password = "oracle_4U";

        // SQL query to get column information
        String tableName = "PMT_TXN"; // Specify the table name here
        String query = "SELECT * FROM payment_order.pmt_txn WHERE 1=0";

        // Print result to file
        String file = "oracle_table_" + tableName + ".json";

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
                JSONObject columnInfo = new JSONObject(); // JSON object to hold column information

                String columnName = metaData.getColumnName(i);
                String columnType = metaData.getColumnTypeName(i);
                int columnSize = metaData.getColumnDisplaySize(i);
                boolean isNullable = (metaData.isNullable(i) == ResultSetMetaData.columnNullable);

                // Populate JSON object with column information
                columnInfo.put("name", columnName);
                columnInfo.put("type", columnType);
                columnInfo.put("size", columnSize);
                columnInfo.put("nullable", isNullable);

                // Add column information to the JSON array
                columnsArray.put(columnInfo);
            }

            // Add the JSON array of columns to the table JSON object
            tableInfo.put(tableName, columnsArray);

            // Write JSON object to file
            try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
                writer.println(tableInfo.toString(4)); // Indented JSON with 4 spaces
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
