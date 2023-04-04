package org.sunbird.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraPropertyReader {

  private static Logger logger = LoggerFactory.getLogger(CassandraPropertyReader.class);
  private final Properties properties = new Properties();
  private static final String file = "cassandratablecolumn.properties";
  private static CassandraPropertyReader cassandraPropertyReader = null;

  /** private default constructor */
  private CassandraPropertyReader() {
    InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
    try {
      properties.load(in);
    } catch (IOException e) {
      logger.error("Error in properties cache", e);
    }
  }

  public static CassandraPropertyReader getInstance() {
    if (null == cassandraPropertyReader) {
      synchronized (CassandraPropertyReader.class) {
        if (null == cassandraPropertyReader) {
          cassandraPropertyReader = new CassandraPropertyReader();
        }
      }
    }
    return cassandraPropertyReader;
  }

  /**
   * Method to read value from resource file .
   *
   * @param key property value to read
   * @return value corresponding to given key if found else will return key itself.
   */
  public String readProperty(String key) {
    return properties.getProperty(key) != null ? properties.getProperty(key) : key;
  }
}
