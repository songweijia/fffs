package edu.cornell.cs.sa;

import edu.cornell.cs.blog.*;

public class HybridLogicalClock implements Comparable<HybridLogicalClock>
{
  public long r;
  public long c;
  
  public HybridLogicalClock() {
	  this.r = 0;
	  this.c = 0;
  }
  
  public HybridLogicalClock(HybridLogicalClock hlc) {
    this.r = hlc.r;
    this.c = hlc.c;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HybridLogicalClock))
      return false;
    
    HybridLogicalClock hlc = (HybridLogicalClock) obj;
    
    return ((hlc.r == this.r) && (hlc.c == this.c));
  }
  
  @Override
  public int compareTo(HybridLogicalClock hlc) {
    if (this.r == hlc.r)
      return Long.compare(this.c, hlc.c);
    return Long.compare(this.r, hlc.r);
  }
  
  synchronized public void tick() {
    long previous_r = r;
    long rtc = JNIBlog.readLocalRTC();
    
    r = Math.max(previous_r, rtc);
    if (r == previous_r)
      c++;
    else
      c = 0;
  }

  synchronized public HybridLogicalClock tickAndCopy(){
    tick();
    return new HybridLogicalClock(this);
  }

  synchronized public void mergeOnRecv(HybridLogicalClock mhlc) {
    if (compareTo(mhlc) < 0) {
      this.r = mhlc.r;
      this.c = mhlc.c;
    } else {
      mhlc.r = this.r;
      mhlc.c = this.c;
    }
  }
  
  synchronized public void tickOnRecv(HybridLogicalClock mhlc) {
    long previous_r = this.r;
    long rtc = JNIBlog.readLocalRTC();
    
    this.r = Math.max(this.r, Math.max(mhlc.r, rtc));
    if ((this.r == previous_r) && (this.r == mhlc.r))
      this.c = Math.max(this.c, mhlc.c) + 1;
    else if (this.r == previous_r)
      this.c++;
    else if (this.r == mhlc.r)
      this.c = mhlc.c + 1;
    else
      this.c = 0;
    mhlc.r = this.r;
    mhlc.c = this.c;
  }
  
  synchronized public HybridLogicalClock tickOnRecvCopy(HybridLogicalClock mhlc){
    tickOnRecv(mhlc);
    return new HybridLogicalClock(this);
  }
  
  synchronized public void tickOnRecvWriteBack(HybridLogicalClock mhlc){
    tickOnRecv(mhlc);
    mhlc.c = this.c;
    mhlc.r = this.r;
  }
  
  synchronized public void mockTick(long rtc) {
    long previous_r = r;
    
    r = Math.max(previous_r, rtc);
    if (r != previous_r)
      c = 0;
  }

  @Override
  public String toString() {
    Long r = new Long(this.r);
    Long c = new Long(this.c);
  
    return "(" + r.toString() + "," + c.toString() + ")";
  }
}

