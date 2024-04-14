package vn.bnh.datadiff;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import vn.bnh.datadiff.controller.DatabaseController;
import vn.bnh.datadiff.controller.FileReaderController;
import vn.bnh.datadiff.controller.QueryController;
import vn.bnh.datadiff.dto.DBObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Application {
    Logger logger = Logger.getLogger(Application.class.getName());
    FileReaderController fileReaderController = new FileReaderController();
    DatabaseController databaseController = new DatabaseController();

    QueryController queryController = new QueryController();

    final String mysqlQuery= "SELECT table_name FROM information_schema.tables WHERE table_schema = ";
    final String oracleQuery = "SELECT table_name FROM all_tables WHERE owner = ";

    public void run(String filePath) throws SQLException {
        logger.info("Start running application");
        Cache<String, DBObject> cacheDBObject = Caffeine.newBuilder()
                .maximumSize(100)
                .build();

        Properties properties = fileReaderController.readFilePath(filePath);

        DBObject mysqlObject = fileReaderController.createMysqlObject(properties);
        DBObject oracleObject = fileReaderController.createOracleObject(properties);

        databaseController.connectMysql(mysqlObject);
        databaseController.connectOracle(oracleObject);

        cacheDBObject.put("mysql", mysqlObject);
        cacheDBObject.put("oracle", oracleObject);

//        ArrayList<String> mysqlTableList =  queryController.getTableList(databaseController.connectMysql(mysqlObject), mysqlObject.getSchemaList(), cacheDBObject, mysqlQuery, "mysql");
        ArrayList<String> oracleTableList =  queryController.getTableList(databaseController.connectOracle(oracleObject), oracleObject.getSchemaList(), cacheDBObject, oracleQuery,"oracle");

        for (String table : oracleTableList) {
            logger.info("Table: " + table);
        }
    }

}
