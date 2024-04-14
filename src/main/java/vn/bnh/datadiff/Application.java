package vn.bnh.datadiff;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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

    final String mysqlTableMetadataQuery = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_KEY, IS_NULLABLE, COLUMN_TYPE FROM information_schema.columns WHERE";

    public void run(String filePath) throws SQLException, FileNotFoundException {
        PrintWriter writer = new PrintWriter("Mismatch_table_between_mysql_and_oracle.txt");
        PrintWriter writer2 = new PrintWriter("Mismatch_table_between_oracle_and_mysql.txt");
        PrintWriter writer3 = new PrintWriter("report.csv");

        //Create a cache object
        logger.info("Start running application");
        Cache<String, DBObject> cacheDBObject = Caffeine.newBuilder()
                .maximumSize(100)
                .build();

        //Read file path
        Properties properties = fileReaderController.readFilePath(filePath);


        //Create Oracle and Mysql object
        DBObject mysqlObject = fileReaderController.createMysqlObject(properties);
        DBObject oracleObject = fileReaderController.createOracleObject(properties);


        //Connect to Oracle and Mysql
        Statement mysqlStatement = databaseController.connectMysql(mysqlObject);
        Statement oracleStatement = databaseController.connectOracle(oracleObject);


        //Put Oracle and Mysql object to cache
        cacheDBObject.put("mysql", mysqlObject);
        cacheDBObject.put("oracle", oracleObject);


        //Get table list from Oracle and Mysql
        ArrayList<String> mysqlTableList = queryController.getTableList(mysqlStatement, mysqlObject.getSchemaList(), cacheDBObject, mysqlQuery, "mysql");
        ArrayList<String> oracleTableList = queryController.getTableList(oracleStatement, oracleObject.getSchemaList(), cacheDBObject, oracleQuery, "oracle");

        //Validate table list between MySQL and Oracle and save the result to a file

        for (String table : validatorController.validateTableList(mysqlTableList, oracleTableList)) {
            writer.println(table);
        }
        writer.close();

        for (String table : validatorController.validateTableList(oracleTableList, mysqlTableList)) {
            writer2.println(table);
        }
        writer2.close();

        String mysqlMetadataQuery = mysqlTableMetadataQuery + " TABLE_SCHEMA = '" + mysqlObject.getSchemaList()[0] + "'" + " AND TABLE_NAME = '" + mysqlTableList.get(0)+"'";
        //Get table metadata from Oracle and Mysql
        JSONObject mysqlMetadata = queryController.getTableMetadata(mysqlMetadataQuery, mysqlStatement, "mysql");

        System.out.println(mysqlMetadata.toString());

    }

}
