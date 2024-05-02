package vn.bnh.datadiff;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;



public class Main {
    static Logger log4j = LogManager.getLogger(Main.class);

    /**
     * This class is the main class of the application. The application will receive 2 arguments from the command line.
     *
     * @arg 0: The path to the properties file
     * @arg 1: The feature that the application will run. The application will run the feature based on the value of this argument.
     */
    public static void main(String[] args) {
        try {
            File file = new File(args[0]);
            String feature = args[1];
            Application application = new Application();
            switch (feature) {
                case "1":
                    application.runValidateMetadata(file);
                    break;
                case "2":
                    application.runMissingTable(file);
                    break;
                case "3":
                    application.runConstrainsAndIndexes(file);
                case "4":
                    application.verifySchema(file);
            }
        } catch (Exception e) {
            log4j.error(e);
        }
    }
}