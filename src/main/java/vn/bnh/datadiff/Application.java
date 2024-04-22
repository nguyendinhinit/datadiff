package vn.bnh.datadiff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.controllers.FileProcessorController;
import vn.bnh.datadiff.controllers.ObjectCreatorController;
import vn.bnh.datadiff.controllers.ProcessorController;
import vn.bnh.datadiff.controllers.QueryController;
import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.dto.DBObject;

import java.util.*;

public class Application {

    FileProcessorController fileProcessorController = new FileProcessorController();
    ObjectCreatorController objectCreatorController = new ObjectCreatorController();
    ProcessorController processor = new ProcessorController();
    QueryController queryController = new QueryController();
    Logger log4j = LogManager.getLogger(Application.class);

    public void runValidateMetadata(String filePath) {
        //Start the Application

        log4j.info("Start the validate application");
        /*
         *File path is the properties file. So in the first time application run, load and read the properties file
         */

        Properties fileProperties = fileProcessorController.readPropertiesFile(filePath);


        /* Create source and destination database object
         * 1. Get connection string, username, password, dbname from the properties file
         * 2. Create source and destination database object
         */

        String srcConnectionString = fileProperties.getProperty("src_connection_string");
        String srcDbName = fileProperties.getProperty("src_dbname");
        String srcUsername = fileProperties.getProperty("src_username");
        String srcPassword = fileProperties.getProperty("src_password");
        DBObject srcDBObject = objectCreatorController.create(srcConnectionString, srcUsername, srcPassword, srcDbName);

        String destConnectionString = fileProperties.getProperty("dest_connection_string");
        String destDbName = fileProperties.getProperty("dest_dbname");
        String destUserName = fileProperties.getProperty("dest_username");
        String destPassword = fileProperties.getProperty("dest_password");
        DBObject destDBObject = objectCreatorController.create(destConnectionString, destUserName, destPassword, destDbName);

        /*
         * Get query from the properties file
         * If query is not null, get metadata of source and destination database by query
         * Else get metadata of source and destination database by default
         */

        String query = fileProperties.getProperty("query");
        LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> srcMetadata;
        LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> destMetadata;

        if (query != null) {
            srcMetadata = queryController.getDbMetadata(srcDBObject, query);
            destMetadata = queryController.getDbMetadata(destDBObject, query);
        } else {
            srcMetadata = queryController.getDbMetadata(destDBObject);
            destMetadata = queryController.getDbMetadata(destDBObject);
        }


        //Compare to metadata of source and destination database
        processor.compare(srcMetadata, destMetadata);

        //Count constrains, index

        Map<String, Integer[]> srcConsAndIds = processor.countConstrainsAndIndexes(srcDBObject);
        Map<String, Integer[]> destConsAndIds = processor.countConstrainsAndIndexes(destDBObject);

        processor.printConstrainsAndIndexes(srcConsAndIds, destConsAndIds);


        /*
        Validate metadata of all table of all schema was listed in the schemas.txt file
         */
    }

    public void runCountJob(String filePath) {
    }
}