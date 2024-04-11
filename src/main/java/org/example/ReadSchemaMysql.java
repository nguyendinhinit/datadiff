package org.example;

import java.io.*;
import java.sql.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

public class ReadSchemaMysql {
    static String mysqlConnectionString;
    static String mysqlUsername;
    static String mysqlPassword;
    static String[] tableNames;

    public static void main(String[] args) throws SQLException {
        String filePath = "application.properties";

        try {
            Properties properties = readPropertiesFile(filePath);
            mysqlConnectionString = properties.getProperty("mysql_connection_string");
            mysqlUsername = properties.getProperty("mysql_username");
            mysqlPassword = properties.getProperty("mysql_password");
            tableNames = properties.getProperty("table_name").split(",\\s*");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Statement stmt = createConnection();
        for (String tableName : tableNames) {
            System.out.printf("Table: %s\n", tableName);
            getTableSchema(stmt, tableName);
        }

        stmt.close();
    }

    private static Properties readPropertiesFile(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(fileName)) {
            properties.load(inputStream);
        }
        return properties;
    }

    public static Statement createConnection() throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(mysqlConnectionString, mysqlUsername, mysqlPassword);
            return conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
            return null;
    }

    public static void getTableSchema(Statement stmt, String tableName) throws SQLException {
        String query = "SELECT * FROM " + tableName + " LIMIT 0";
        String file = "mysql_table_" + tableName + ".json";

        JSONObject tableInfo = new JSONObject(); // JSON object to hold table information
        JSONArray columnsArray = new JSONArray(); // JSON array to hold column information

        ResultSet rs = stmt.executeQuery(query);

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
            e.printStackTrace();
        }
    }
}
