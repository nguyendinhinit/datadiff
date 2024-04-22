package vn.bnh.datadiff.service.Impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import vn.bnh.datadiff.service.QueryService;
import vn.bnh.datadiff.service.ValidatorService;

import java.io.*;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ValidatorServiceImpl implements ValidatorService {
    Logger log4j = LogManager.getLogger(ValidatorServiceImpl.class);

    QueryService queryService = new QueryServiceImpl();

    DatabaseServiceImpl databaseService = new DatabaseServiceImpl();
    DataTypeMapperImpl dataTypeMapper = new DataTypeMapperImpl();

    @Override
    public ArrayList<String> validateTableList(ArrayList<String> source, ArrayList<String> desc, String schema) {
        //Find mismatch table list between Source and Desc database and return the list of mismatch table names
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

    @Override
    public ArrayList<String> validateTableList(Map<String, String> informationMap, int compareOption) {
        //Find mismatch table list between Source and Desc database and return the list of mismatch table names
        LinkedHashMap<String, ArrayList<String>> sourceTables = new LinkedHashMap<>();
        LinkedHashMap<String, ArrayList<String>> descTables = new LinkedHashMap<>();
        BufferedReader bufferedReader = null;
        ArrayList<String> tableList = new ArrayList<>();
        try {
            bufferedReader = new BufferedReader(new FileReader("schemas.txt"));
            String line = bufferedReader.readLine();
            while (line != null) {
                String schema = line;
                line = bufferedReader.readLine();
                log4j.info("Schema: " + schema);
                ArrayList<String> sourceTable = queryService.getTableList(informationMap.get("source_connection_string"), informationMap.get("source_type"), informationMap.get("source_username"), informationMap.get("source_password"), schema);
                ArrayList<String> descTable = queryService.getTableList(informationMap.get("destination_connection_string"), informationMap.get("destination_type"), informationMap.get("destination_username"), informationMap.get("destination_password"), schema);
                sourceTables.put(schema, sourceTable);
                descTables.put(schema, descTable);
                //compare sourceTable and descTable, if table exist in both source and desc, add to arrayList
                for (String table : sourceTable) {
                    if (descTable.contains(table)) {
                        tableList.add(table);
                    }
                }
            }
        } catch (Exception e) {
            log4j.error("Error: " + e);
        } finally {
            try {
                bufferedReader.close();
            } catch (Exception e) {
                log4j.error("Error: " + e);
            }
        }

        switch (compareOption) {
            case 1:
                String fileName1 = "missing_table_between_source_and_desc.txt";
                createMissingTableReport(sourceTables, descTables, fileName1);
                break;
            case 2:
                String fileName2 = "missing_table_between_desc_and_source.txt";
                createMissingTableReport(descTables, sourceTables, fileName2);
                break;
            default:
                log4j.error("Invalid compare option");
        }
        log4j.info("Table List: " + tableList);
        return tableList;
    }

    public void createMissingTableReport(LinkedHashMap<String, ArrayList<String>> sourceTables, LinkedHashMap<String, ArrayList<String>> descTables, String fileName) {
        log4j.info("Source Tables: " + sourceTables);
        log4j.info("Desc Tables: " + descTables);
        ArrayList<String> missingTable = new ArrayList<>();
        String schema = null;
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName, true))) {
            for (Map.Entry<String, ArrayList<String>> entry : sourceTables.entrySet()) {
                schema = entry.getKey();
                ArrayList<String> sourceTable = entry.getValue();
                ArrayList<String> descTable = descTables.get(schema);
                missingTable = validateTableList(sourceTable, descTable, schema);
                log4j.info("Missing Table: " + missingTable);
                if (missingTable != null) {
                    for (String table : missingTable) {
                        log4j.info("Missing Table: " + table);
                        bufferedWriter.write(table + "\n");
                    }
                }
            }
        } catch (Exception e) {
            log4j.error("Error: " + e);
        }
    }

    @Deprecated
    public void createCSVReport(JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName) throws FileNotFoundException {
        //Create CSV report

        PrintWriter writer = new PrintWriter(new FileOutputStream(new File("report.csv"), true /* append = true */));

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

//        writer.write("Schema Name,Table Name,Source Column Name,Source Data Type,Source Length,Source Precision,Source Scale,Source Nullable,Source Key,Source Data Default,Destination Column Name,Destination Data Type,Destination Length,Destination Precision,Destination Scale,Destination Nullable,Destination Key,Destination Data Default,Column Name Validator,Data Type Validator,Length Validator,Precision Validator,Scale Validator,Nullable Validator,Key Validator,Data Default\n");
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

                    Map<String, String[]> dataTypeMapping = DataTypeMapperImpl.getDataTypeMapping();

                    //Validator
                    String columnNameValidator;
                    if (sourceColumnName.equals(descColumnName)) columnNameValidator = "TRUE";
                    else columnNameValidator = "FALSE";

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
                    if (sourceLength.equals(descLength)) lengthValidator = "TRUE";
                    else lengthValidator = "FALSE";

                    String precisionValidator;
                    if (sourcePrecision.equals(descPrecision)) precisionValidator = "TRUE";
                    else precisionValidator = "FALSE";

                    String scaleValidator;
                    if (sourceScale.equals(descScale)) scaleValidator = "TRUE";
                    else scaleValidator = "FALSE";

                    String nullableValidator;
                    String descNull = String.valueOf(descNullable.charAt(0));
                    if (sourceNullable.equals(descNull)) nullableValidator = "TRUE";
                    else nullableValidator = "FALSE";

                    String keyValidator;
                    if (sourceKey.equals(descKey)) keyValidator = "TRUE";
                    else keyValidator = "FALSE";

                    String dataDefaultValidator;
                    if (sourceDataDefault.equals(descDataDefault)) dataDefaultValidator = "TRUE";
                    else dataDefaultValidator = "FALSE";

                    writer.write(sourceColumnSchema + "," + sourceColumnTableName + "," + sourceColumnName + "," + sourceDataType + "," + sourceLength + "," + sourcePrecision + "," + sourceScale + "," + sourceNullable + "," + sourceKey + "," + sourceDataDefault + "," + descColumnName + "," + descDataType + "," + descLength + "," + descPrecision + "," + descScale + "," + descNullable + "," + descKey + "," + descDataDefault + "," + columnNameValidator + "," + dataTypeValidator + "," + lengthValidator + "," + precisionValidator + "," + scaleValidator + "," + nullableValidator + "," + keyValidator + "," + dataDefaultValidator + "\n");
                }
            }
        }
        writer.close();
    }

    public void createCSVReport(String table, String sourceConnectionString, String source_username, String source_password, String descConnectionString, String desc_username, String desc_password) {
        String schema = table.split("\\.")[0];
        String tbl = table.split("\\.")[1];
        String oracleQuery = "SELECT COLUMN_NAME, DATA_TYPE,DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM all_tab_columns WHERE OWNER = '";
        String mysqlQuery = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,COLUMN_KEY,COLUMN_DEFAULT,DATETIME_PRECISION  FROM information_schema.columns WHERE TABLE_SCHEMA = '";
        File file = new File("report.csv");

        if (!file.exists()) {
            try {
                PrintWriter writer = new PrintWriter(file);
                writer.write("Schema Name,Table Name,Source Column Name,Source Data Type,Source Length,Source Precision,Source Scale,Source Nullable,Source Data Default,Source Key,Source Index Name,Source Sequence,Destination Column Name,Destination Data Type,Destination Length,Destination Precision,Destination Scale,Destination Nullable,Destination Key,Destination Index Name,Destination Auto Increment,Destination Data Default,Column Name Validator,Data Type Validator,Length Validator,Precision Validator,Scale Validator,Nullable Validator,Key Validator,Index Name Validator,Incremental Validator,Data Default\n");
                writer.close();
            } catch (Exception e) {
                log4j.error("Error: " + e);
            }
        }

        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream(new File("report.csv"), true /* append = true */));
            log4j.info("Starting compare table : " + tbl + " at schema: " + schema);
            Statement stmtSource = databaseService.connectToDatabase(sourceConnectionString, source_username, source_password);
            Statement stmtDesc = databaseService.connectToDatabase(descConnectionString, desc_username, desc_password);

            try {
                // Index Name Validator
                HashMap<String, String> srcIndexes = new HashMap<>();
                HashMap<String, String> destIndexes = new HashMap<>();

                // Get Source Indexes
                String getSrcIndexesQuery = String.format("SELECT * FROM ALL_IND_COLUMNS WHERE TABLE_OWNER = '%s' AND TABLE_NAME = '%s'", schema, tbl);
                ResultSet srcIndexesResultSet = stmtSource.executeQuery(getSrcIndexesQuery);
                log4j.info("Src indexes query : " + getSrcIndexesQuery);
                while (srcIndexesResultSet.next()) {
                    srcIndexes.put(srcIndexesResultSet.getString("COLUMN_NAME"), srcIndexesResultSet.getString("INDEX_NAME"));
                }

                // Get Destination Indexes
                String getDestIndexesQuery = String.format("SELECT DISTINCT TABLE_NAME, INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'", schema, tbl);
                ResultSet destIndexesResultSet = stmtDesc.executeQuery(getDestIndexesQuery);
                log4j.info("Dest indexes query : " + getDestIndexesQuery);
                while (destIndexesResultSet.next()) {
                    destIndexes.put(destIndexesResultSet.getString("COLUMN_NAME"), destIndexesResultSet.getString("INDEX_NAME"));
                }

                // Incremental Validator
                ArrayList<String> srcSequences = new ArrayList<>();
                ArrayList<String> destAutoIncrements = new ArrayList<>();

                // Get Source Sequences
                String getSrcSequencesQuery = String.format("SELECT DATA_DEFAULT AS SEQUENCE_VAL , TABLE_NAME , COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = '%s' AND TABLE_NAME = '%s' AND IDENTITY_COLUMN = 'YES'", schema, tbl);
                ResultSet srcSequencesResultSet = stmtSource.executeQuery(getSrcSequencesQuery);
                log4j.info("Src sequences query : " + getSrcSequencesQuery);
                while (srcSequencesResultSet.next()) {
                    srcSequences.add(srcSequencesResultSet.getString("COLUMN_NAME"));
                }

                // Get Destination Auto Increments
                String getDestAutoIncrementsQuery = String.format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' and EXTRA = 'AUTO_INCREMENT'", schema, tbl);
                ResultSet destAutoIncrementsResultSet = stmtDesc.executeQuery(getDestAutoIncrementsQuery);
                log4j.info("Dest auto increments query : " + getDestAutoIncrementsQuery);
                while (destAutoIncrementsResultSet.next()) {
                    destAutoIncrements.add(destAutoIncrementsResultSet.getString("COLUMN_NAME"));
                }

                log4j.info("Start get table metadata from source database");
                ResultSet sourceResultSet = stmtSource.executeQuery(oracleQuery + schema.toUpperCase() + "' AND TABLE_NAME = '" + tbl.toUpperCase() + "' order by COLUMN_NAME");
                log4j.info("Start get table metadata from desc database");
                ResultSet descResultSet = stmtDesc.executeQuery(mysqlQuery + schema.toUpperCase() + "' AND TABLE_NAME = '" + tbl.toUpperCase() + "' ORDER BY COLUMN_NAME collate utf8_bin");
                log4j.info(mysqlQuery + schema.toUpperCase() + "' AND TABLE_NAME = '" + tbl.toUpperCase() + "' ORDER BY COLUMN_NAME collate utf8_bin");
                if (sourceResultSet.getRow() != descResultSet.getRow()) {
                    log4j.warn("Found different number of columns in source and desc database at table: " + tbl + " in schema: " + schema);
                    return;
                }
                while (sourceResultSet.next() && descResultSet.next()) {
                    String sourceColumnName = sourceResultSet.getString("COLUMN_NAME");
                    String sourceDataType = sourceResultSet.getString("DATA_TYPE");
                    String sourceLength = sourceResultSet.getString("DATA_LENGTH");
                    String sourcePrecision = sourceResultSet.getString("DATA_PRECISION");
                    String sourceScale = sourceResultSet.getString("DATA_SCALE");
                    String sourceNullable = sourceResultSet.getString("NULLABLE");
                    String sourceDataDefault = sourceResultSet.getString("DATA_DEFAULT");

                    if (sourceDataDefault != null) {
                        sourceDataDefault = sourceDataDefault.trim();
                    }
                    ArrayList<String> sourcePrimaryKeys = queryService.findPk(tbl, schema, sourceConnectionString, source_username, source_password);
                    String sourcePrimaryKey = "";

                    if (!sourcePrimaryKeys.isEmpty() && sourcePrimaryKeys.contains(sourceColumnName)) {
                        sourcePrimaryKey = "PRI";
                    }

                    String descColumnName = descResultSet.getString("COLUMN_NAME");
                    String descDataType = descResultSet.getString("DATA_TYPE");
                    String descLength = descResultSet.getString("CHARACTER_MAXIMUM_LENGTH");
                    String descPrecision = descResultSet.getString("NUMERIC_PRECISION");
                    String descScale = descResultSet.getString("NUMERIC_SCALE");
                    String descNullable = String.valueOf(descResultSet.getString("IS_NULLABLE").charAt(0));
                    String descKey = descResultSet.getString("COLUMN_KEY");
                    String descDataDefault = descResultSet.getString("COLUMN_DEFAULT");
                    String descDateTimePrecision = descResultSet.getString("DATETIME_PRECISION");

                    if (descDateTimePrecision != null) {
                        descDataType = descDataType + "(" + descDateTimePrecision + ")";
                    }

                    // Mapping index name based on column name
                    String srcIndexName = srcIndexes.get(sourceColumnName);
                    String destIndexName = destIndexes.get(descColumnName);

                    // Mapping increment based on column name
                    String isSrcSequence = String.valueOf(srcSequences.contains(sourceColumnName));
                    String isDestAutoIncrement = String.valueOf(destAutoIncrements.contains(descColumnName));

                    //Start validate
                    writer.write(schema + "," + tbl + "," + sourceColumnName + "," + sourceDataType + "," + sourceLength + "," + sourcePrecision + "," + sourceScale + "," + sourceNullable + "," + sourceDataDefault + "," + sourcePrimaryKey + "," + srcIndexName + "," + isSrcSequence + "," + descColumnName + "," + descDataType + "," + descLength + "," + descPrecision + "," + descScale + "," + descNullable + "," + descKey + "," + destIndexName + "," + isDestAutoIncrement + "," + descDataDefault + ",");

                    log4j.info("append: schema: " + schema + ", tbl: " + tbl + ", source col name: " + sourceColumnName + ", source data type: " + sourceDataType + ", source length: " + sourceLength + ", source percision: " + sourcePrecision + ", source scale: " + sourceScale + ", source nullable: " + sourceNullable + ", source data default: " + sourceDataDefault + ", source pk: " + sourcePrimaryKey + ", source index" + srcIndexName + ", source sequence" + isSrcSequence + ", desc col name: " + descColumnName + ", desc data type:" + descDataType + ", desc len: " + descLength + ", desc precision: " + descPrecision + ", desc scale: " + descScale + ", desc nullable: " + descNullable + ", desc key: " + descKey + ", desc index: " + destIndexName + ", desc auto increment: " + isDestAutoIncrement + ", desc data default: " + descDataDefault);

                    if (validateColumn("col name", sourceColumnName, descColumnName)) writer.write("TRUE,");
                    else writer.write("FALSE,");

                    String[] mapping = dataTypeMapper.getDataTypeMapping().get(sourceDataType);

                    if (mapping != null) {
                        for (String descType : mapping) {
                            if (descType.equalsIgnoreCase(descDataType)) {
                                log4j.info("Validate data type with Source Type:{} , Desc type: {} .Status: TRUE", sourceDataType, descDataType.toUpperCase());
                                writer.write("TRUE,");
                                break;
                            } else {
                                log4j.info("Validate data type with Source Type:" + sourceDataType + ", Desc type: " + descDataType.toUpperCase() + " Status: FALSE");
                                writer.write("FALSE,");
                                break;
                            }
                        }
                    }

                    if (validateColumn("col len", sourceLength, descLength)) writer.write("TRUE,");
                    else writer.write("FALSE,");

                    if (validateColumn("col precision", sourcePrecision, descPrecision)) writer.write("TRUE,");
                    else writer.write("FALSE,");

                    if (validateColumn("col scale", sourceScale, descScale)) writer.write("TRUE,");
                    else writer.write("FALSE,");

                    if (validateColumn("col nullable", sourceNullable, descNullable)) writer.write("TRUE,");
                    else writer.write("FALSE,");

                    if (validateColumn("col pk", sourcePrimaryKey, descKey)) writer.write("TRUE,");
                    else writer.write("FALSE,");

                    if (validateColumn("col index", srcIndexName, destIndexName)) writer.write("TRUE,");
                    else writer.write("FALSE,");

                    if (validateColumn("col increment", isSrcSequence, isDestAutoIncrement)) writer.write("TRUE\n");
                    else writer.write("FALSE\n");

                    log4j.info("Write to report.csv");
                }
                sourceResultSet.close();
                descResultSet.close();
                stmtSource.close();
                stmtDesc.close();
                writer.close();
            } catch (Exception e) {
                log4j.error("Error: " + e);
            }
        } catch (Exception e) {
            log4j.error("Error: " + e);
        }
    }

    @Override
    public boolean validateColumn(String columnName, String src, String desc) {
        //Validate Column
        if (src != null && desc != null) {
            if (src.equals(desc)) {
                log4j.info("validateColumn {} with src value: {}, dest value: {} is true", columnName, src, desc);
                return true;
            } else {
                log4j.info("validateColumn {} with src value: {}, dest value: {} is false", columnName, src, desc);
                return false;
            }
        } else if (src == null && desc == null) {
            log4j.info("validateColumn {} with src value: {}, dest value: {} is true", columnName, src, desc);
            return true;
        } else {
            log4j.info("validateColumn {} with src value: {}, dest value: {} is false", columnName, src, desc);
            return false;
        }
    }

    public void validateKey(JSONObject source, JSONObject desc, ArrayList<String> sourceTableList, ArrayList<String> descTableList, String schemaName) {
        //Validate Key
        String mysqlCheckKeyQuery;
        String oracleCheckKeyQuery;
    }
}
