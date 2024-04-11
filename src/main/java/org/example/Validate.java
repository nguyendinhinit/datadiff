package org.example;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class Validate{
    static String [] tableNames;
    public static void main(String[] args) throws IOException {
        String filePath = "application.properties";

        try {
            Properties properties = readPropertiesFile(filePath);
            tableNames = properties.getProperty("table_name").split(",\\s*");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Reporting

        for(String tableName : tableNames) {
            try {
                JSONObject mysqlJson = readJsonFile("mysql_table_" + tableName + ".json");
                JSONObject oracleJson = readJsonFile("oracle_table_" + tableName + ".json");
                System.out.println(tableName);
                compareSchemas(mysqlJson, oracleJson, tableName);
                System.out.printf("Comparison report for table %s has been generated in %s\n", tableName, "report_table_"+tableName);
            } catch (IOException | JSONException e) {
                e.printStackTrace();
            }
        }
    }

    private static JSONObject readJsonFile(String filename) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(filename)));
        return new JSONObject(content);
    }

    private static void compareSchemas(JSONObject mysqlJson, JSONObject oracleJson, String tableName) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter("report_table_"+tableName));
        writer.println("Comparison Report for table: " + tableName );
        Map<String, String []> dataTypeMapping = DataTypeMapper.getDataTypeMapping();

        try {
            JSONArray mysqlArray = mysqlJson.getJSONArray(tableName);
            JSONArray oracleArray = oracleJson.getJSONArray(tableName);

            // Assuming both arrays are of equal length
            for (int i = 0; i < mysqlArray.length(); i++) {
                JSONObject mysqlObject = mysqlArray.getJSONObject(i);
                JSONObject oracleObject = oracleArray.getJSONObject(i);
                String mysqlType = mysqlObject.getString("type");
                String oracleType = oracleObject.getString("type");
                Integer mysqlSize = mysqlObject.getInt("size");
                Integer oracleSize = oracleObject.getInt("size");
                boolean mysqlNullable = mysqlObject.getBoolean("nullable");
                boolean oracleNullable = oracleObject.getBoolean("nullable");
                String mysqlName = mysqlObject.getString("name");
                String oracleName = oracleObject.getString("name");

                // Compare column names
                if (!mysqlName.equals(oracleName)) {
                    writer.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    writer.println("MySQL name: " + mysqlName);
                    writer.println("Oracle name: " + oracleName);
                }
                // Compare column sizes
                if (!mysqlSize.equals(oracleSize)) {
                    writer.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    writer.println("MySQL size: " + mysqlSize);
                    writer.println("Oracle size: " + oracleSize);
                }
                // Compare column nullability
                if (mysqlNullable != oracleNullable) {
                    writer.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    writer.println("MySQL nullable: " + mysqlNullable);
                    writer.println("Oracle nullable: " + oracleNullable);
                }

                String[] oracleTypes = dataTypeMapping.get(oracleType);

                if (oracleTypes == null) {
                    writer.println("No mapping found for MySQL type: " + mysqlType);
                    continue;
                }
                if (Arrays.stream(oracleTypes).noneMatch(mysqlType::equals)) {
                    writer.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    writer.println("MySQL type: " + mysqlType);
                    writer.println("Oracle type: " + oracleType);
                }


            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        writer.close();
    }
    private static Properties readPropertiesFile(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(fileName)) {
            properties.load(inputStream);
        }
        return properties;
    }
}


