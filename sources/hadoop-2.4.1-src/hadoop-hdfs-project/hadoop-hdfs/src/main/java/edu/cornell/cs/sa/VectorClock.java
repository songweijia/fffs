/**
 * 
 */
package edu.cornell.cs.sa;

/**
 * @author sonic
 *
 */
public class VectorClock
{
  static public int numProc = 16; // 16 is the default number: remember to initialized this to a configuratio  
  public long vcs[]; // vector clocks, the first item is the number of nodes.
  public int pid;

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof VectorClock))
    return false;

    VectorClock vco = (VectorClock) obj;
    if(vcs == null || vco.vcs == null ||
        vcs.length == 0 || vco.vcs.length == 0)
      return false;

    for(int i=0;i<numProc;i++)
      if(vcs[i]!=vco.vcs[i])
        return false;
    
    return true;
  }

  public VectorClock(int pid){
    this.vcs = new long[numProc];
    this.pid = pid;
  }
      
  public VectorClock(){
    this(-1);
  }
	
  public VectorClock(VectorClock vco){
    this();
    synchronized(vco){
      pid = vco.pid;
      System.arraycopy(vco.vcs, 0, vcs, 0, numProc);
    }
  }

  public long getVectorClockValue(int pid){
    return vcs[pid];
  }
  
  synchronized public VectorClock tickAndCopy() {
    if(pid>=0)//if pid < 0, we don't tick.
      vcs[pid]++;
    return new VectorClock(this);
  }

  synchronized public void tick() {
    if(pid>=0)//if pid < 0, we don't tick.
      vcs[pid]++;
  }

  synchronized public void tickOnRecv(VectorClock mvc){
    tick();
    for(int i=0;i<numProc;i++)
      vcs[i]=(vcs[i]>mvc.vcs[i])?vcs[i]:mvc.vcs[i];
  }

  synchronized public void tickOnRecvWriteBack(VectorClock mvc){
    tick();
    for(int i=0;i<numProc;i++)
      mvc.vcs[i] = vcs[i] = Math.max(vcs[i], mvc.vcs[i]);
  }

  /**
   * Get Consistent Clock
   * @param vcArr
   * @return
   */
  public static VectorClock getCCC(VectorClock[] vcArr){
    if(vcArr==null || vcArr.length == 0)
      return null;
    VectorClock rvc = new VectorClock(-1);
    for(VectorClock vc: vcArr)
      for(int i=0;i<numProc;i++)
        rvc.vcs[i] = Math.max(rvc.vcs[i], vc.vcs[i]);
    return rvc;
  }
  
  public String toString(){
    StringBuffer sb = new StringBuffer("{");
    for(int i=0;i<numProc;i++){
      if(i == pid)sb.append('*');
      sb.append(vcs[i]);
      if( i < numProc - 1 )sb.append(',');
    }
    sb.append("}");
    return sb.toString();
  }
  
  static public void main(String args[]){
    VectorClock.numProc = 3;
    VectorClock vcs [] = new VectorClock[3];
    vcs[0] = new VectorClock(0);
    vcs[0].tick();
    vcs[1] = new VectorClock(1);
    vcs[2] = new VectorClock(2);
    vcs[2].tick();
    vcs[2].tickOnRecvWriteBack(vcs[0]);
    for(int i=0;i<3;i++)
      System.out.println(vcs[i]);
    System.out.println(VectorClock.getCCC(vcs));
    System.out.println(vcs[2].tickAndCopy());
  }
}

