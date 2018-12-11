package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.EnumSet;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public abstract class SeekableDFSOutputStream extends FSOutputSummer 
implements Syncable, CanSetDropBehind{
  protected SeekableDFSOutputStream(Checksum sum, int maxChunkSize, int checksumSize) {
    super(sum, maxChunkSize, checksumSize);
    // TODO Auto-generated constructor stub
  }
  /**
   * get Replication number.
   * @return
   * @throws IOException
   */
  public abstract int getCurrentBlockReplication() throws IOException;
  /**
   * 
   * @param syncFlags
   * @throws IOException
   */
  public abstract void hsync(EnumSet<SyncFlag> syncFlags)throws IOException;
  /**
   * flush previous data and seek to specified file position.
   * @param pos
   * @throws IOException
   */
  public abstract void seek(long pos)throws IOException; 
  /**
   * get Position of the current write cursor.
   * @return
   * @throws IOException
   */
  public abstract long getPos() throws IOException;
  
  /**
   * get Initial length of the package.
   * @return
   */
  public abstract long getInitialLen();
  
  /**
   * @throws IOException
   */
  public abstract void abort() throws IOException;
  
  /**
   * get block
   * @return
   */
  public abstract ExtendedBlock getBlock();
}
