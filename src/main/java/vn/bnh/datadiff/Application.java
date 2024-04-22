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

        log4j.info("Start the Application");

        // File path is the properties file. So in the first time application run, load and read the properties file


        Properties fileProperties = fileProcessorController.readPropertiesFile(filePath);

        // Create sourceDbObject and DestDbObject
        //Load object properties


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


        //Save all schema metadata to LinkedHashMap include schema name, table name, table column metadata
        LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> srcMetadata;
        srcMetadata = queryController.getDbMetadata(srcDBObject);

        LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> destMetadata;
        destMetadata = queryController.getDbMetadata(destDBObject);

        //Compare to metadata of source and destination database
        processor.compare(srcMetadata, destMetadata);

        /*
        Validate metadata of all table of all schema was listed in the schemas.txt file
         */
    }

    public void runCountJob(String filePath) {
    }
}