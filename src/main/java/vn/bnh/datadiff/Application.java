package vn.bnh.datadiff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.controllers.FileProcessorController;
import vn.bnh.datadiff.controllers.ObjectCreatorController;
import vn.bnh.datadiff.controllers.ProcessorController;
import vn.bnh.datadiff.controllers.QueryController;
import vn.bnh.datadiff.dto.ColumnObject;
import vn.bnh.datadiff.dto.DBObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This class provides methods to validate metadata between source and destination databases.
 * It reads properties from a file to establish database connections and compare their metadata.
 * Currently supports counting constraints and indexes and printing them.
 *
 * @author NguyenND
 * @version 0.1
 * @since 2024-04-23
 */
public class Application {

    FileProcessorController fileProcessorController = new FileProcessorController();
    ObjectCreatorController objectCreatorController = new ObjectCreatorController();
    ProcessorController processor = new ProcessorController();
    QueryController queryController = new QueryController();
    Logger log4j = LogManager.getLogger(Application.class);

    /**
     * Validates metadata between source and destination databases.
     * Reads properties from the specified file path to establish connections.
     * Compares metadata and prints constraints and indexes of both databases.
     *
     * @param filePath The path to the properties file containing connection information.
     *                 The properties file should include connection strings, usernames, passwords,
     *                 and database names of the source and destination databases.
     * @throws IOException If there is an error reading the properties file.
     */
    public void runValidateMetadata(String filePath) {
        log4j.info("Start the validate application");
        System.out.println(".------------------------------------------.\n" + "|            _ _     _       _             |\n" + "|__   ____ _| (_) __| | __ _| |_ ___  _ __ |\n" + "|\\ \\ / / _` | | |/ _` |/ _` | __/ _ \\| '__||\n" + "| \\ V / (_| | | | (_| | (_| | || (_) | |   |\n" + "|  \\_/ \\__,_|_|_|\\__,_|\\__,_|\\__\\___/|_|   |\n" + "'------------------------------------------'");
        // Read properties file
        Properties fileProperties = fileProcessorController.readPropertiesFile(filePath);


        // Create source database objects
        DBObject srcDBObject = objectCreatorController.create(fileProperties, "src");

        // Create destination database objects
        DBObject destDBObject = objectCreatorController.create(fileProperties, "dest");

        //Read query from properties file
        String query = fileProperties.getProperty("query");

        //Create src and dest dbMetadata
        LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> srcMetadata;
        LinkedHashMap<String, Map<String, ArrayList<ColumnObject>>> destMetadata;

        //Get metadata of source and destination database
        //If query is not null, get metadata of source and destination database by query
        if (query != null) {
            srcMetadata = queryController.getDbMetadata(srcDBObject, query);
            destMetadata = queryController.getDbMetadata(destDBObject, query);
        } else {
            srcMetadata = queryController.getDbMetadata(srcDBObject);
            destMetadata = queryController.getDbMetadata(destDBObject);
        }

        //Compare to metadata of source and destination database
        processor.compare(srcMetadata, destMetadata);

        //Count constrains, index
        Map<String, Integer[]> srcConsAndIds = processor.countConstrainsAndIndexes(srcDBObject);
        Map<String, Integer[]> destConsAndIds = processor.countConstrainsAndIndexes(destDBObject);

        //Print constrains, index
        processor.printConstrainsAndIndexes(srcConsAndIds, destConsAndIds);
    }

    public void runCountJob(String filePath) {
    }

    public void runMissingTable(String filePath) {
        log4j.info("Start the validate application");
        // Read properties file
        Properties fileProperties = fileProcessorController.readPropertiesFile(filePath);


        // Create source database objects
        DBObject srcDBObject = objectCreatorController.create(fileProperties, "src");

        // Create destination database objects
        DBObject destDBObject = objectCreatorController.create(fileProperties, "dest");

        //Read query from properties file
        ArrayList<String> srcSchemaList = queryController.getSchema(srcDBObject);
        ArrayList<String> destSchemaList = queryController.getSchema(destDBObject);
        processor.foundMissingTable(srcDBObject, destDBObject);


    }

    public void runConstrainsAndIndexes(String filePath) {
        Properties fileProperties = fileProcessorController.readPropertiesFile(filePath);
        // Create source database objects
        DBObject srcDBObject = objectCreatorController.create(fileProperties, "src");
        // Create destination database objects
        DBObject destDBObject = objectCreatorController.create(fileProperties, "dest");

        Map<String, Map<String, ArrayList<Integer>>> srcObjectMetadata;
        Map<String, Map<String, ArrayList<Integer>>> destObjectMetadata;

        srcObjectMetadata = queryController.getObjectMetadata(srcDBObject);
        destObjectMetadata = queryController.getObjectMetadata(destDBObject);

        processor.objectLevelCompare(srcObjectMetadata, destObjectMetadata);

    }

}