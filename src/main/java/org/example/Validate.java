package org.example;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class Validate{
    static String report_file="report.txt";
    static String tableName = "pmt_txn";
    public static void main(String[] args) throws IOException {
        String mysqlJsonFile = "mysql_table_pmt_txn.json";
        String oracleJsonFile = "oracle_table_PMT_TXN.json";

        // Reporting

        try {
            JSONObject mysqlJson = readJsonFile(mysqlJsonFile);
            JSONObject oracleJson = readJsonFile(oracleJsonFile);

            compareSchemas(mysqlJson, oracleJson);
        } catch (IOException | JSONException e) {
            e.printStackTrace();
        }
    }

    private static JSONObject readJsonFile(String filename) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(filename)));
        return new JSONObject(content);
    }

    private static void compareSchemas(JSONObject mysqlJson, JSONObject oracleJson) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(report_file));
        writer.println("Comparison Report for table: " + tableName);
        Map<String, String> dataTypeMapping = DataTypeMapper.getDataTypeMapping();

        try {
            JSONArray mysqlArray = mysqlJson.getJSONArray("pmt_txn");
            JSONArray oracleArray = oracleJson.getJSONArray("PMT_TXN");

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
                    System.out.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    System.out.println("MySQL name: " + mysqlName);
                    System.out.println("Oracle name: " + oracleName);
                }
                // Compare column sizes
                if (!mysqlSize.equals(oracleSize)) {
                    System.out.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    System.out.println("MySQL size: " + mysqlSize);
                    System.out.println("Oracle size: " + oracleSize);
                }
                // Compare column nullability
                if (mysqlNullable == oracleNullable) {
                    System.out.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    System.out.println("MySQL nullable: " + mysqlNullable);
                    System.out.println("Oracle nullable: " + oracleNullable);
                }
                // Map MySQL data type to Oracle data type
                String mappedOracleType = dataTypeMapping.get(mysqlType);
                // Compare mapped data types
                if(mappedOracleType == null) {
                    System.out.println("No mapping found for MySQL type: " + mysqlType);
                    continue;
                }
                if (!mappedOracleType.equals(oracleType)) {
                    System.out.println("Mismatch found for column: " + mysqlObject.getString("name"));
                    System.out.println("MySQL type: " + mysqlType);
                    System.out.println("Oracle type: " + oracleType);
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        writer.close();
    }
}


