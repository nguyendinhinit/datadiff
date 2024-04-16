package vn.bnh.datadiff;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.json.JSONArray;
import org.json.JSONObject;
import vn.bnh.datadiff.controller.DatabaseController;
import vn.bnh.datadiff.controller.FileReaderController;
import vn.bnh.datadiff.controller.QueryController;
import vn.bnh.datadiff.controller.ValidatorController;
import vn.bnh.datadiff.dto.DBObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Application {
    Logger logger = Logger.getLogger(Application.class.getName());
    FileReaderController fileReaderController = new FileReaderController();
    DatabaseController databaseController = new DatabaseController();

    QueryController queryController = new QueryController();
    ValidatorController validatorController = new ValidatorController();

    final String mysqlQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = ";
    final String oracleQuery = "SELECT table_name FROM all_tables WHERE owner = ";

    final String mysqlTableMetadataQuery = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,COLUMN_KEY,COLUMN_DEFAULT  FROM information_schema.columns WHERE";
    final String oracleTableMetadataQuery = "SELECT OWNER, TABLE_NAME,COLUMN_NAME, DATA_TYPE,DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM all_tab_columns WHERE OWNER = '";

    public void run(String filePath) throws SQLException, FileNotFoundException {
        //check file if exist
        File file = new File("schema_list.txt");



        PrintWriter writer = new PrintWriter(new FileOutputStream(
                new File("Mismatch_table_between_mysql_and_oracle.txt"),
                true /* append = true */));

        PrintWriter writer2 = new PrintWriter(new FileOutputStream(
                new File("Mismatch_table_between_oracle_and_mysql.txt"),
                true /* append = true */));

//        PrintWriter writer = new PrintWriter("Mismatch_table_between_mysql_and_oracle.txt");
//        PrintWriter writer2 = new PrintWriter("Mismatch_table_between_oracle_and_mysql.txt");

        //Create a cache object
        logger.info("Start running application");
        Cache<String, DBObject> cacheDBObject = Caffeine.newBuilder().maximumSize(100).build();

        //Read file path
        Properties properties = fileReaderController.readFilePath(filePath);


        //Create Oracle and Mysql object from properties file
        DBObject mysqlObject = fileReaderController.createMysqlObject(properties);
        DBObject oracleObject = fileReaderController.createOracleObject(properties);


        //Connect to Oracle and Mysql
        Statement mysqlStatement = databaseController.connectToDatabase(mysqlObject);
        Statement oracleStatement = databaseController.connectToDatabase(oracleObject);


        //Put Oracle and Mysql object to cache
        cacheDBObject.put("mysql", mysqlObject);
        cacheDBObject.put("oracle", oracleObject);


        if (!file.exists()) queryController.getAllSchema(oracleObject);



        //Get table list from Oracle and Mysql
        ArrayList<String> mysqlTableList = queryController.getTableList(mysqlStatement, mysqlObject.getSchemaList(), cacheDBObject, mysqlQuery, "mysql");
        ArrayList<String> oracleTableList = queryController.getTableList(oracleStatement, oracleObject.getSchemaList(), cacheDBObject, oracleQuery, "oracle");

        //Validate table list between MySQL and Oracle and save the result to a file

        for (String schema : mysqlObject.getSchemaList()) {
            for (String table : validatorController.validateTableList(mysqlTableList, oracleTableList, schema)) {
                writer.println(table);
            }
            writer.close();

            for (String table : validatorController.validateTableList(oracleTableList, mysqlTableList, schema)) {
                writer2.println(table);
            }
            writer2.close();
        }

        JSONObject mysqlSchemaMetaData = new JSONObject();
        JSONObject oracleSchemaMetaData = new JSONObject();

        ArrayList<String> pK = new ArrayList<>();
        for (String schema : oracleObject.getSchemaList()) {
            for (String table : oracleTableList) {
                pK = queryController.findPk(oracleStatement, table.split("\\.")[1], schema.toUpperCase());
            }
        }

        mysqlSchemaMetaData = queryController.getSchemaMetaData(mysqlObject, mysqlStatement, "mysql", mysqlTableMetadataQuery, mysqlTableList, pK);
        oracleSchemaMetaData = queryController.getSchemaMetaData(oracleObject, oracleStatement, "oracle", oracleTableMetadataQuery, oracleTableList, pK);


        //Create CSV report for Oracle and Mysql table metadata
        for (String schema : oracleObject.getSchemaList()) {
            validatorController.createCSVReport(oracleSchemaMetaData, mysqlSchemaMetaData, oracleTableList, mysqlTableList, schema);
        }

        //Count job
        Map<String, String> oracleJobList = new HashMap<>();
        Map<String, String> mysqlJobList = new HashMap<>();
        for (String schema : mysqlObject.getSchemaList()) {
            mysqlJobList = queryController.countJob(mysqlStatement, schema, "mysql");
        }
//        PrintWriter writer4 = new PrintWriter("job_count.csv");

        PrintWriter writer4 = new PrintWriter(new FileOutputStream(
                new File("job_count.csv"),
                true /* append = true */));

        writer4.write("Schema Name,Oracle TABLE, VIEW,TRIGGER,FUNCTION,PROCEDURE,SCHEDULE,Mysql TABLE, VIEW,TRIGGER,FUNCTION,PROCEDURE,SCHEDULE\n");
        for (String schema : oracleObject.getSchemaList()) {
            oracleJobList = queryController.countJob(oracleStatement, schema, "oracle");
            mysqlJobList = queryController.countJob(mysqlStatement, schema, "mysql");

            writer4.write(schema+",");
            if (oracleJobList.get("TABLE") != null) {
                writer4.write(oracleJobList.get("TABLE") + ",");
            } else {
                writer4.write("0,");
            }

            if (oracleJobList.get("VIEW") != null) {
                writer4.write(oracleJobList.get("VIEW") + ",");
            } else {
                writer4.write("0,");
            }

            if (oracleJobList.get("TRIGGER") != null) {
                writer4.write(oracleJobList.get("TRIGGER") + ",");
            } else {
                writer4.write("0,");
            }

            if (oracleJobList.get("FUNCTION") != null) {
                writer4.write(oracleJobList.get("FUNCTION") + ",");
            } else {
                writer4.write("0,");
            }

            if (oracleJobList.get("PROCEDURE") != null) {
                writer4.write(oracleJobList.get("PROCEDURE") + ",");
            } else {
                writer4.write("0,");
            }

            if (oracleJobList.get("SCHEDULE") != null) {
                writer4.write(oracleJobList.get("SCHEDULE") + ",");
            } else {
                writer4.write("0,");
            }

            if (mysqlJobList.get("TABLE") != null) {
                writer4.write(mysqlJobList.get("TABLE") + ",");
            } else {
                writer4.write("0,");
            }

            if (mysqlJobList.get("VIEW") != null) {
                writer4.write(mysqlJobList.get("VIEW") + ",");
            } else {
                writer4.write("0,");
            }

            if (mysqlJobList.get("TRIGGER") != null) {
                writer4.write(mysqlJobList.get("TRIGGER") + ",");
            } else {
                writer4.write("0,");
            }

            if (mysqlJobList.get("FUNCTION") != null) {
                writer4.write(mysqlJobList.get("FUNCTION") + ",");
            } else {
                writer4.write("0,");
            }

            if (mysqlJobList.get("PROCEDURE") != null) {
                writer4.write(mysqlJobList.get("PROCEDURE") + ",");
            } else {
                writer4.write("0,");
            }

            if (mysqlJobList.get("SCHEDULE") != null) {
                writer4.write(mysqlJobList.get("SCHEDULE") + "\n");
            } else {
                writer4.write("0\n");
            }

            writer4.close();
        }





    }

}
