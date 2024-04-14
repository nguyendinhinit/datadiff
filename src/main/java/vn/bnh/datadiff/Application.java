package vn.bnh.datadiff;

import vn.bnh.datadiff.controller.DatabaseController;
import vn.bnh.datadiff.controller.FileReaderController;
import vn.bnh.datadiff.controller.QueryController;
import vn.bnh.datadiff.dto.MysqlObject;
import vn.bnh.datadiff.dto.OracleObject;

import java.util.Properties;
import java.util.logging.Logger;

public class Application {
    Logger logger = Logger.getLogger(Application.class.getName());
    FileReaderController fileReaderController = new FileReaderController();
    DatabaseController databaseController = new DatabaseController();

    QueryController queryController = new QueryController();

    public void run(String filePath) {
        logger.info("Start running application");
        Properties properties = fileReaderController.readFilePath(filePath);

        MysqlObject mysqlObject = fileReaderController.createMysqlObject(properties);
        OracleObject oracleObject = fileReaderController.createOracleObject(properties);

        databaseController.connectMysql(mysqlObject);
        databaseController.connectOracle(oracleObject);

        queryController.getTableList(databaseController.connectMysql(mysqlObject), mysqlObject.getSchemaList());
    }

}
