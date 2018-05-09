package edu.cornell.cs.perf;

public class PerformanceTraceSwitch {
  static private final boolean DATANODE_TIME_BREAKDOWN = false;
  static private final boolean DATANODE_TIME_BREAKDOWN_NO_WRITE = false;
  static private final boolean PACKET_TIMESTAMP = false;
  
  static public boolean getDataNodeTimeBreakDown(){
    return DATANODE_TIME_BREAKDOWN;
  }
  static public boolean getDataNodetimeBreakDownNoWrite(){
    return DATANODE_TIME_BREAKDOWN_NO_WRITE;
  }
  static public boolean getPacketTimestamp(){
    return PACKET_TIMESTAMP;
  }
}
