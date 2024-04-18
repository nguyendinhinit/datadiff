package vn.bnh.datadiff;

import vn.bnh.datadiff.controllers.FileProcessorController;
import vn.bnh.datadiff.controllers.ObjectCreatorController;
import vn.bnh.datadiff.dto.DbObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class Application {

    FileProcessorController fileProcessorController = new FileProcessorController();
    ObjectCreatorController objectCreatorController = new ObjectCreatorController();
    public void runValidateMetadata(String filePath){
        // File path is the properties file. So in the first time application run, load and read the properties file

        Properties fileProperties =fileProcessorController.readPropertiesFile(filePath);

        // Create sourceDbOject and DestDbObject
        //Load object properties
        String srcConnectionString = fileProperties.getProperty("src_connection_string");
        String srcDbName = fileProperties.getProperty("src_dbname");
        String srcUsername = fileProperties.getProperty("src_username");
        String srcPassword = fileProperties.getProperty("src_password");
        DbObject srcDBOject = objectCreatorController.create(srcConnectionString,srcUsername,srcPassword,srcDbName);

        String destConnectionString = fileProperties.getProperty("dest_connection_string");
        String destDbName = fileProperties.getProperty("dest_dbname");
        String destUserName = fileProperties.getProperty("dest_username");
        String destPassword = fileProperties.getProperty("dest_password");
        DbObject destDBObject = objectCreatorController.create(destConnectionString,destUserName,destPassword,destDbName);

        // Custom query to get schema
        if(fileProperties.containsKey("query")) {

        }

        //Save all schema metadata to LinkedHashMap include schema name, table name, table column metadata
        LinkedHashMap<String, Map<String, ArrayList<String>>> srcMetadata;
        LinkedHashMap<String, Map<String, ArrayList<String>>> destMetadata;
        /*
        Validate metadata of all table of all schema was listed in the schemas.txt file
         */
    }

    public void runCountJob(String filePath){}
}