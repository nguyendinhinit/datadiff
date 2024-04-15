package vn.bnh.datadiff.service;

import org.json.JSONArray;
import org.json.JSONObject;
import vn.bnh.datadiff.dto.DBObject;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ValidatorService {

    public ArrayList<String> validateTableList(ArrayList<String> source, ArrayList<String> desc, String schema) {
        //Find mismatch table list between Oracle and Mysql database and return the list of mismatch table names
        ArrayList<String> tableList = new ArrayList<>();
        if (source.size() == 0 || desc.size() == 0) {
            return tableList;
        }
        for (String table : source) {
            if (!desc.contains(table)) {
                tableList.add(table);
            }
        }

        return tableList;
    }

    public void createCSVReport(JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException {
        //Create CSV report
        PrintWriter writer = new PrintWriter("report.csv");
        String tableName;
        JSONArray sourceArray = source.getJSONArray(schemaName);
        JSONArray descArray = desc.getJSONArray(schemaName);
        Map<String, JSONObject> sourceList = new HashMap<>();
        Map<String, JSONObject> descList = new HashMap<>();
        for (int i = 0; i < sourceArray.length(); i++) {
            tableName = sourceTableList.get(i).split("\\.")[1];
            JSONObject sourceTable = sourceArray.getJSONObject(i);
            sourceList.put(tableName, sourceTable);

        }

        for (int i = 0; i < descArray.length(); i++) {
            tableName = descTableList.get(i).split("\\.")[1];
            JSONObject descTable = descArray.getJSONObject(i);
            descList.put(tableName, descTable);
        }

        writer.write("Schema Name,Table Name,Source Column Name,Source Data Type,Source Length,Source Precision,Source Scale,Source Nullable,Source Key,Source Data Default,Destination Column Name,Destination Data Type,Destination Length,Destination Precision,Destination Scale,Destination Nullable,Destination Key,Destination Data Default,Column Name Validator,Data Type Validator,Length Validator,Precision Validator,Scale Validator,Nullable Validator,Key Validator,Data Default\n");
        for (String table : sourceList.keySet()) {
            if (descList.containsKey(table)) {
                JSONObject sourceTable = sourceList.get(table);
                JSONObject descTable = descList.get(table);

                JSONArray sourceColumns = sourceTable.getJSONArray(table);
                JSONArray descColumns = descTable.getJSONArray(table.toLowerCase());
                for (int i = 0; i < sourceColumns.length(); i++) {
                    JSONObject sourceColumnMetadata = sourceColumns.getJSONObject(i);
                    JSONObject descColumnMetadata = descColumns.getJSONObject(i);
                    String sourceColumnSchema = sourceColumnMetadata.getString("TABLE_SCHEMA");
                    String sourceColumnTableName = sourceColumnMetadata.getString("TABLE_NAME");
                    String sourceColumnName = sourceColumnMetadata.getString("COLUMN_NAME");
                    String sourceDataType = sourceColumnMetadata.getString("DATA_TYPE");
                    String sourceLength = sourceColumnMetadata.getString("CHARACTER_MAXIMUM_LENGTH");
                    String sourcePrecision = sourceColumnMetadata.getString("NUMERIC_PRECISION");
                    String sourceScale = sourceColumnMetadata.getString("NUMERIC_SCALE");
                    String sourceNullable = sourceColumnMetadata.getString("IS_NULLABLE");
                    String sourceKey = sourceColumnMetadata.getString("COLUMN_KEY");
                    String sourceDataDefault = sourceColumnMetadata.getString("COLUMN_DEFAULT");

                    String descColumnName = descColumnMetadata.getString("COLUMN_NAME");
                    String descDataType = descColumnMetadata.getString("DATA_TYPE");
                    String descLength = descColumnMetadata.getString("CHARACTER_MAXIMUM_LENGTH");
                    String descPrecision = descColumnMetadata.getString("NUMERIC_PRECISION");
                    String descScale = descColumnMetadata.getString("NUMERIC_SCALE");
                    String descNullable = descColumnMetadata.getString("IS_NULLABLE");
                    String descKey = descColumnMetadata.getString("COLUMN_KEY");
                    String descDataDefault = descColumnMetadata.getString("COLUMN_DEFAULT");

                    Map<String, String[]> dataTypeMapping = DataTypeMapper.getDataTypeMapping();

                    //Validator
                    String columnNameValidator;
                    if (sourceColumnName.equals(descColumnName))
                        columnNameValidator = "TRUE";
                    else
                        columnNameValidator = "FALSE";

                    String dataTypeValidator = "FALSE";
                    String[] descDataTypeArray = dataTypeMapping.get(sourceDataType);
                    if (descDataTypeArray != null) {
                        for (String descType : descDataTypeArray) {
                            if (descType.toUpperCase().equals(descDataType.toUpperCase())) {
                                dataTypeValidator = "TRUE";
                                break;
                            } else {
                                dataTypeValidator = "FALSE";
                            }
                        }
                    } else {
                        dataTypeValidator = "FALSE";
                    }


                    String lengthValidator;
                    if (sourceLength.equals(descLength))
                        lengthValidator = "TRUE";
                    else
                        lengthValidator = "FALSE";

                    String precisionValidator;
                    if (sourcePrecision.equals(descPrecision))
                        precisionValidator = "TRUE";
                    else
                        precisionValidator = "FALSE";

                    String scaleValidator;
                    if (sourceScale.equals(descScale))
                        scaleValidator = "TRUE";
                    else
                        scaleValidator = "FALSE";

                    String nullableValidator;
                    String descNull = String.valueOf(descNullable.charAt(0));
                    if (sourceNullable.equals(descNull))
                        nullableValidator = "TRUE";
                    else
                        nullableValidator = "FALSE";

                    String keyValidator;
                    if (sourceKey.equals(descKey))
                        keyValidator = "TRUE";
                    else
                        keyValidator = "FALSE";

                    String dataDefaultValidator;
                    if (sourceDataDefault.equals(descDataDefault))
                        dataDefaultValidator = "TRUE";
                    else
                        dataDefaultValidator = "FALSE";

                    writer.write(sourceColumnSchema + "," + sourceColumnTableName + "," + sourceColumnName + "," + sourceDataType + "," + sourceLength + "," + sourcePrecision + "," + sourceScale + "," + sourceNullable + "," + sourceKey + "," + sourceDataDefault + "," + descColumnName + "," + descDataType + "," + descLength + "," + descPrecision + "," + descScale + "," + descNullable + "," + descKey + "," + descDataDefault + "," + columnNameValidator + "," + dataTypeValidator + "," + lengthValidator + "," + precisionValidator + "," + scaleValidator + "," + nullableValidator + "," + keyValidator + "," + dataDefaultValidator+"\n");
                }


            }
        }
        writer.close();
    }

    public void validateKey(JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName){
        //Validate Key
        String mysqlCheckKeyQuery;
        String oracleCheckKeyQuery;

    }
}
