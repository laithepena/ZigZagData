package conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ReportingVerticaConnect {
    // init database constants
    private static final String DATABASE_DRIVER = "com.vertica.Driver";
    private static final String DATABASE_NAME="reportinghub";
    
    private static final String DATABASE_URL = "jdbc:vertica://qa-us1-rhub01a.blackarrow-corp.com:5433/"+DATABASE_NAME;
    
    // private static final String DATABASE_URL = "jdbc:mysql://qa-us1-fc01.blackarrow-corp.com:3306/"+DATABASE_NAME;
    private static final String USERNAME = "qa17";
    private static final String PASSWORD = "password123";
    private static final String MAX_POOL = "250";

    // init connection object
    private Connection connection;
    // init properties object
    private Properties properties;

    // create properties
    private Properties getProperties() {
        if (properties == null) {
            properties = new Properties();
            properties.setProperty("user", USERNAME);
            properties.setProperty("password", PASSWORD);
            properties.setProperty("MaxPooledStatements", MAX_POOL);
        }
        return properties;
    }

    // connect database
    public Connection connect() {
        if (connection == null) {
            try {
                Class.forName(DATABASE_DRIVER);
                connection = DriverManager.getConnection(DATABASE_URL, getProperties());
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    // disconnect database
    public void disconnect() {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}