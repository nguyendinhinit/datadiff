package org.example;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class Application {
    public void run(String[] args) throws SQLException, IOException {
        System.out.println(
                " ____       _                              \n" +
                        "/ ___|  ___| |__   ___ _ __ ___   __ _     \n" +
                        "\\___ \\ / __| '_ \\ / _ \\ '_ ` _ \\ / _` |    \n" +
                        " ___) | (__| | | |  __/ | | | | | (_| |    \n" +
                        "|____/ \\___|_| |_|\\___|_| |_|_|_|\\__,_|    \n" +
                        "\\ \\   / /_ _| (_) __| | __ _| |_ ___  _ __ \n" +
                        " \\ \\ / / _` | | |/ _` |/ _` | __/ _ \\| '__|\n" +
                        "  \\ V / (_| | | | (_| | (_| | || (_) | |   \n" +
                        "   \\_/ \\__,_|_|_|\\__,_|\\__,_|\\__\\___/|_|   "
        );
        ReadSchemaOracle.main(args);
        ReadSchemaMysql.main(args);
        Validator.main(args);
    }
}
