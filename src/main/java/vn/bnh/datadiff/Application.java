package vn.bnh.datadiff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.controllers.FileProcessorController;
import vn.bnh.datadiff.controllers.ObjectCreatorController;
import vn.bnh.datadiff.controllers.QueryController;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.dto.TableObject;

import java.util.*;

public class Application {

    FileProcessorController fileProcessorController = new FileProcessorController();
    ObjectCreatorController objectCreatorController = new ObjectCreatorController();

    QueryController queryController = new QueryController();
    Logger log4j = LogManager.getLogger(Application.class);

    public void runValidateMetadata(String filePath) {
        //Start the Application

        log4j.info("Start the Application");

        // File path is the properties file. So in the first time application run, load and read the properties file


        Properties fileProperties = fileProcessorController.readPropertiesFile(filePath);

        // Create sourceDbOject and DestDbObject
        //Load object properties


        String srcConnectionString = fileProperties.getProperty("src_connection_string");
        String srcDbName = fileProperties.getProperty("src_dbname");
        String srcUsername = fileProperties.getProperty("src_username");
        String srcPassword = fileProperties.getProperty("src_password");
        DBObject srcDBOject = objectCreatorController.create(srcConnectionString, srcUsername, srcPassword, srcDbName);

        String destConnectionString = fileProperties.getProperty("dest_connection_string");
        String destDbName = fileProperties.getProperty("dest_dbname");
        String destUserName = fileProperties.getProperty("dest_username");
        String destPassword = fileProperties.getProperty("dest_password");
        DBObject destDBObject = objectCreatorController.create(destConnectionString, destUserName, destPassword, destDbName);


//        // Custom query to get schema
//        ArrayList<String> srcSchemaList = new ArrayList<>();
//        ArrayList<String> destSchemaList = new ArrayList<>();
//        if (fileProperties.containsKey("query")) {
//            String query = fileProperties.getProperty("query");
//            srcSchemaList = queryController.getSchema(srcDBOject, query);
//            destSchemaList = queryController.getSchema(destDBObject, query);
//
//        } else {
//            srcSchemaList = queryController.getSchema(srcDBOject);
//        }

        //Save all schema metadata to LinkedHashMap include schema name, table name, table column metadata
        LinkedHashMap<String, Map<String, ArrayList<TableObject>>> srcMetadata;
        srcMetadata = queryController.getDbMetadata(srcDBOject);

        LinkedHashMap<String, Map<String, ArrayList<String>>> destMetadata;
//        destMetadata = queryController.getDbMetadata(destDBObject,schemaList);


        /*
        Validate metadata of all table of all schema was listed in the schemas.txt file
         */
    }

    public void runCountJob(String filePath) {
    }
}