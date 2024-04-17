package vn.bnh.datadiff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.bnh.datadiff.controller.FileCheckerController;
import vn.bnh.datadiff.controller.FileReaderController;
import vn.bnh.datadiff.controller.QueryController;
import vn.bnh.datadiff.controller.ValidatorController;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

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


        //Start read properties file
        Properties inputProperties = fileReaderController.readPropertiesFile(filePath);

        Map<String, String> informationMap = inputProperties.entrySet().stream()
                .collect(toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));

        log4j.info("Properties: " + informationMap);

        //Save the schema list to a file
        /*Todo: Uncoment when build the application
       queryController.getAllSchema(informationMap.get("query"), informationMap.get("source_connection_string"), informationMap.get("source_type"), informationMap.get("source_username"), informationMap.get("source_password"));
         */

        //Start comparing the schema between source and destination database
        ArrayList<String> tableList = new ArrayList<>();
       tableList = validatorController.validateTableList(informationMap,1);
       validatorController.validateTableList(informationMap,2);


       //Create csv report will return a metadata different report between source and destination database

        ArrayList<String> test =new ArrayList<>();
        test.add("PAYMENT_ORDER.ARRANGEMENT_GROUP");
        test.add("PAYMENT_ORDER.PMT_TXN_ADD_PROP");
        for (String table : test) {
            validatorController.createCSVReport(table, informationMap.get("source_connection_string"), informationMap.get("source_username"), informationMap.get("source_password"), informationMap.get("destination_connection_string"), informationMap.get("destination_username"), informationMap.get("destination_password"));
        }

        //End Application
        log4j.info("Application ended");

    }


}
