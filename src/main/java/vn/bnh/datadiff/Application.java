package vn.bnh.datadiff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.controller.FileCheckerController;
import vn.bnh.datadiff.controller.FileReaderController;
import vn.bnh.datadiff.controller.QueryController;
import vn.bnh.datadiff.controller.ValidatorController;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toMap;


public class Application {

    Logger log4j = LogManager.getLogger(Application.class);

    FileCheckerController fileCheckerController = new FileCheckerController();

    FileReaderController fileReaderController = new FileReaderController();

    ValidatorController validatorController = new ValidatorController();
    QueryController queryController = new QueryController();
    public void run(String filePath) {
        //Start Application
        log4j.info("Application started");

        BufferedReader bufferedReader;

        //Start read properties file
        Properties inputProperties =  fileReaderController.readPropertiesFile(filePath);

        Map<String, String> informationMap = inputProperties.entrySet().stream()
                .collect(toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));

        log4j.info("Properties: " + informationMap);

        //Save the schema list to a file
        /*Todo: Uncoment when build the application
       queryController.getAllSchema(informationMap.get("query"), informationMap.get("source_connection_string"), informationMap.get("source_type"), informationMap.get("source_username"), informationMap.get("source_password"));
         */

        //Start comparing the schema between source and destination database
        //Find missing table from source and desc
        try{
            bufferedReader = new BufferedReader(new FileReader("schemas.txst"));
            String line= bufferedReader.readLine();
            while (line != null){
              String schema = line;
                queryController.getTableList(informationMap.get("source_connection_string"), informationMap.get("source_type"), informationMap.get("source_username"), informationMap.get("source_password"), schema);
                queryController.getTableList(informationMap.get("destination_connection_string"), informationMap.get("destination_type"), informationMap.get("destination_username"), informationMap.get("destination_password"), schema);
                line = bufferedReader.readLine();
            }
        }catch (Exception e){
            log4j.error("Error: " + e);
        }
//        queryController.getTableList(informationMap.get("source_connection_string"), informationMap.get("source_type"), informationMap.get("source_username"), informationMap.get("source_password"));
        //Find missing table from desc and source

    }


}
