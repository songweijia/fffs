package edu.cornell.cs.blog;

import java.util.Map;
import java.util.HashMap;

public class RecordParserFactory {
  
  private final static Map<String,String> rpMap;

  static{
    rpMap = new HashMap<String,String>();
    rpMap.put(null,DefaultRecordParser.class.getName());
    rpMap.put("ts64",ts64RecordParser.class.getName());
    rpMap.put("pmu",PMURecordParser.class.getName());
    rpMap.put("se",PMURecordParser.class.getName());
  }

  /**
   * Create an IRecordParser object by suffix key.
   */
  public static IRecordParser getRecordParser(String suffix) throws Exception {
    ClassLoader cl = RecordParserFactory.class.getClassLoader();
    return (IRecordParser) (cl.loadClass(rpMap.get(suffix)).newInstance());
  }
}
