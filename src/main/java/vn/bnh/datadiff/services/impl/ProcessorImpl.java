package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.mapping.DataTypeMapper;
import vn.bnh.datadiff.services.Processor;
import vn.bnh.datadiff.services.QueryService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class ProcessorImpl implements Processor {

    Logger log4j = LogManager.getLogger(ProcessorImpl.class);

    Map<String, String[]> mappings = DataTypeMapper.getDataTypeMapping();

    QueryService queryService = new QueryServiceImpl();

    @Override
    public void compare(LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> srcDbMetadata, LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> destDbMetadata) {
        log4j.info("Create report file");
        // Create report file
        try (FileWriter fw = new FileWriter("report.csv");
             BufferedWriter bw = new BufferedWriter(fw)) {
            String header = "schemaName,tableName,srcColumnName,srcDataType,srcDataLength,srcDataPrecision,srcDataScale,srcNullable,srcDataDefault,srcPrimaryKey,srcIncremental,destColumnName,destDataType,destDataLength,destDataPrecision,destDataScale,destNullable,destDataDefault,destPrimaryKey,destIncremental,validateColumn,validateDataType,validateDataLength,validateDataPrecision,validateDataScale,validateNullable,validateDataDefault,validatePrimaryKey,validateIncremental";
            bw.write(header);
            bw.newLine(); // Add a new line after the appended line
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
            log4j.info("Error writing to file report.csv");
        }

        log4j.info("Compare to metadata of source and destination database");
        for (Map.Entry<String, Map<String, ArrayList<ColumnObject>>> entry : srcDbMetadata.entrySet()) {
            if (!destDbMetadata.containsKey(entry.getKey())) {
                log4j.info("Schema " + entry.getKey() + " is different");
                String line = String.format("schema %s not exist in destination database", entry.getKey());
                printToCsv("compare_oracle2mysql.txt", line);
                continue;
            }
            for (Map.Entry<String, ArrayList<ColumnObject>> tableEntry : entry.getValue().entrySet()) {
                if (!destDbMetadata.get(entry.getKey()).containsKey(tableEntry.getKey())) {
                    log4j.info("Table " + tableEntry.getKey() + " is different");
                    String line = String.format("table %s not exist in destination database", tableEntry.getKey());
                    printToCsv("compare_oracle2mysql.txt", line);
                    continue;
                } else if (tableEntry.getValue().size() != destDbMetadata.get(entry.getKey()).get(tableEntry.getKey()).size()) {
                    printToCsv("different_col_between_oracle and_mysql.txt", "Column size is different");
                    continue;
                }
                int columnPosition = 0;
                for (ColumnObject column : tableEntry.getValue()) {
                    String report = reportBuilder(column, destDbMetadata.get(entry.getKey()).get(tableEntry.getKey()).get(columnPosition), mappings);
                    printToCsv("report.csv", report);
                    columnPosition++;
                }
            }
            schemaCompare(entry, destDbMetadata.get(entry.getKey()));
        }
    }


    public void printToCsv(String file, String line) {
        try (FileWriter fw = new FileWriter(file, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(line);
            bw.newLine(); // Add a new line after the appended line
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
            log4j.info("Error writing to file {}", file);
        }
        log4j.info("Print to CSV {} to file {}", line, file);
    }

    public String reportBuilder(ColumnObject srcColumn, ColumnObject destColumn, Map<String, String[]> mappings) {
        String schemaName = srcColumn.getSchemaName();
        String tableName = srcColumn.getTableName();
        String srcColumnName = srcColumn.getColumnName();
        String destColumnName = destColumn.getColumnName();
        String srcDataType = srcColumn.getDataType();
        String destDataType = destColumn.getDataType();
        String srcDataLength = srcColumn.getDataLength();
        String destDataLength = destColumn.getDataLength();
        String srcDataPrecision = srcColumn.getDataPrecision();
        String destDataPrecision = destColumn.getDataPrecision();
        String srcDataScale = srcColumn.getDataScale();
        String destDataScale = destColumn.getDataScale();
        String srcNullable = srcColumn.getNullable();
        String destNullable = destColumn.getNullable();
        String srcDataDefault = srcColumn.getDataDefault();
        String destDataDefault = destColumn.getDataDefault();
        String srcPrimaryKey = srcColumn.getPrimaryKey();
        String destPrimaryKey = destColumn.getPrimaryKey();
        String srcIncremental = srcColumn.getIncremental();
        String destIncremental = destColumn.getIncremental();


        boolean validateColumn = validator(srcColumnName, destColumnName);

        String srcDataTypeMapped = null;
        if (mappings.get(srcDataType) == null) {
            srcDataTypeMapped = srcDataType;
        } else {
            srcDataTypeMapped = mappings.get(srcDataType)[0];
        }

        boolean validateDataType = validator(srcDataTypeMapped, destDataType.toUpperCase());

        boolean validateDataLength = validator(srcDataLength, destDataLength);

        boolean validateDataPrecision = validator(srcDataPrecision, destDataPrecision);

        boolean validateDataScale = validator(srcDataScale, destDataScale);

        boolean validateNullable = validator(srcNullable, destNullable);

        boolean validateDataDefault = validator(srcDataDefault, destDataDefault);

        boolean validatePrimaryKey = validator(srcPrimaryKey, destPrimaryKey);

        boolean validateIncremental = validator(srcIncremental, destIncremental);


        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", schemaName, tableName, srcColumnName, srcDataType, srcDataLength, srcDataPrecision, srcDataScale, srcNullable, srcDataDefault, srcPrimaryKey, srcIncremental, destColumnName, destDataType, destDataLength, destDataPrecision, destDataScale, destNullable, destDataDefault, destPrimaryKey, destIncremental, validateColumn, validateDataType, validateDataLength, validateDataPrecision, validateDataScale, validateNullable, validateDataDefault, validatePrimaryKey, validateIncremental);

    }

    public boolean find() {

        return false;
    }

    public void schemaCompare
            (Map.Entry<String, Map<String, ArrayList<ColumnObject>>> srcSchema, Map<String, ArrayList<ColumnObject>> destSchema) {
        log4j.info("Schema Compare");
    }

    public boolean validator(String src, String dest) {
        log4j.info("Validator {},{}", src, dest);
        if (src == null && dest == null) {
            return true;
        } else if (src == null || dest == null) {
            return false;
        }
        return src.equals(dest);
    }

    @Override
    public Map<String, Integer[]> countConstrainsAndIndexes(DBObject dbObject) {
        return queryService.countConstraintsAndIndexes(dbObject);
    }

    @Override
    public void printConstrainsAndIndexes(Map<String, Integer[]> srcConstrainsAndIndexes, Map<String, Integer[]> destConstrainsAndIndexes) {
        File file = new File("Constrains & Indexes");
        try (FileWriter fw = new FileWriter(file, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            for (Map.Entry<String, Integer[]> entry : srcConstrainsAndIndexes.entrySet()) {
                String line = String.format("%s,%s,%s,%s,%s", entry.getKey(), entry.getValue()[0], entry.getValue()[1], destConstrainsAndIndexes.get(entry.getKey())[0], destConstrainsAndIndexes.get(entry.getKey())[1]);
                bw.write(line);
                bw.newLine(); // Add a new line after the appended line
                bw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
            log4j.info("Error writing to file Constrains & Indexes");
        }
    }
}
