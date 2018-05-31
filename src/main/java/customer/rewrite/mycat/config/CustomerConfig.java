package customer.rewrite.mycat.config;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * 表相关加密的配置类
 */
public class CustomerConfig {

  static Logger logger = LoggerFactory.getLogger(CustomerConfig.class);
  static Properties props = new Properties();
  static Map<String, List<String>> secTableNap = new HashMap<String, List<String>>();
  public static String encryptedSuffix = "_xx";

  static {
    try {
      init();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static List<String> getEncryptField(String tableName) {
    return secTableNap.get(tableName) == null ? Collections.<String>emptyList() : secTableNap.get(tableName);
  }

  private static void init() throws IOException {

    props.load(CustomerConfig.class.getResourceAsStream("/customerConfig.properties"));
    logger.debug("loaded config file:customerConfig.properties");

    Iterator<Object> sets = props.keySet().iterator();
    /***
     * sectable=single:name,age;student:content;
     */
    while (sets.hasNext()) {

      /***
       *key=sectable
       */
      String key = (String) sets.next();

      /***
       * config=single:name,age;student:content;
       */
      String configValue = (props.getProperty(key) == null || props.getProperty(key).length() < 1 ? "" : props.getProperty(key));
      logger.debug("key:" + configValue);
      if (configValue.length() < 1) {
        logger.error("没有任何配置.....configValue=[" + configValue + "]");
        return;
      }
      /***
       * splitTables[0]=single:name,age
       * splitTables[1]=student:content
       */
      String[] splitTables = configValue.split("\\;");
      for (String singleTable : splitTables) {
        /***
         * 0=single
         * 1=name,age
         */
        String[] singleTableConfig = singleTable.split("\\:");
        /***
         * columnConfig[0]=name
         * columnConfig[0]=age
         */
        String[] columnConfig = singleTableConfig[1].split("\\,");
        List<String> columnList = Arrays.asList(columnConfig);

        /***
         * single={name,name}
         * student={content}
         */
        secTableNap.put(singleTableConfig[0], columnList);
        for (String columnName : columnList) {
          logger.debug("configed: [" + singleTableConfig[0] + "]====>[" + columnName + "]");
        }
      }
    }
  }
}
