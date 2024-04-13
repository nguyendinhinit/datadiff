package vn.bnh.datadiff;

import java.util.logging.Logger;

public class Main {
    static Logger logger = Logger.getLogger(Main.class.getName());
    public static void main(String[] args) {
        logger.info("Hello World!");

        // Create Application object and run it
        Application application = new Application();
        application.run();

    }
}
