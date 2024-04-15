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

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;

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
        PrintWriter writer = new PrintWriter("Mismatch_table_between_mysql_and_oracle.txt");
        PrintWriter writer2 = new PrintWriter("Mismatch_table_between_oracle_and_mysql.txt");
        PrintWriter writer3 = new PrintWriter("report.csv");

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

        ArrayList<String> pK=new ArrayList<>();
        for (String schema : oracleObject.getSchemaList()) {
            for (String table : oracleTableList) {
                pK = queryController.findPk(oracleStatement, table.split("\\.")[1], schema.toUpperCase());
                System.out.println(pK.toString());
            }
        }

        mysqlSchemaMetaData = queryController.getSchemaMetaData(mysqlObject, mysqlStatement, "mysql", mysqlTableMetadataQuery, mysqlTableList, pK);
        oracleSchemaMetaData = queryController.getSchemaMetaData(oracleObject, oracleStatement, "oracle", oracleTableMetadataQuery, oracleTableList,pK);


        //Create CSV report for Oracle and Mysql table metadata
        for (String schema : oracleObject.getSchemaList()) {
            validatorController.createCSVReport(oracleSchemaMetaData, mysqlSchemaMetaData, oracleTableList, mysqlTableList, schema);
        }

        for (String schema : mysqlObject.getSchemaList()) {
            validatorController.validateKey(oracleSchemaMetaData, mysqlSchemaMetaData, oracleTableList, mysqlTableList, schema);
        }

        //Count job



        String oracleCountJobQuery = "SELECT a.owner,          COUNT (a.object_name)     AS NoTable,          COUNT (b.object_name)     AS NoView,          COUNT (c.object_name)     AS NoTrigger,          COUNT (d.object_name)     AS NoFunction,          COUNT (e.object_name)     AS NoProcedure,          COUNT (f.object_name)     AS NoSchedule     FROM dba_objects a          LEFT JOIN dba_objects b ON a.owner = b.owner          LEFT JOIN dba_objects c ON a.owner = c.owner          LEFT JOIN dba_objects d ON a.owner = d.owner          LEFT JOIN dba_objects e ON a.owner = e.owner          LEFT JOIN dba_objects f ON a.owner = f.owner    WHERE     a.object_type = 'TABLE'          AND b.object_type = 'VIEW'          AND c.object_type = 'TRIGGER'          AND d.object_type = 'FUNCTION'          AND e.object_type = 'PROCEDURE'          AND f.object_type = 'SCHEDULE' GROUP BY a.owner ORDER BY a.owner;";
        logger.info("Application finished running");


    }

}
