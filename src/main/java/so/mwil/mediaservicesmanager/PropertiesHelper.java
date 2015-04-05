package so.mwil.mediaservicesmanager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by matthew on 05/04/15.
 */
public class PropertiesHelper {

    private Properties properties;

    public PropertiesHelper() throws IOException {

        properties = new Properties();
        String propFileName = "manager.properties";

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            properties.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
