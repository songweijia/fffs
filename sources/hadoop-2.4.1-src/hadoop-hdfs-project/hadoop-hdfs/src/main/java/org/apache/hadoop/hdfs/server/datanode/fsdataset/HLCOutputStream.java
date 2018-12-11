/**
 * 
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.IOException;

import java.io.OutputStream;
import edu.cornell.cs.sa.HybridLogicalClock;;

/**
 * @author sonic
 * Vector Clock aware output stream
 */
abstract public class HLCOutputStream extends OutputStream {

	/**
	 * @param mhlc HybridLogicalClock, Input/Output argument
     * @param blockOffset Offset for current block.
	 * @param buff buffer
	 * @param buffOffset Offset for buffer
	 * @param len length
	 * @throws IOException
	 */
	final public void write(HybridLogicalClock mhlc, long blockOffset, byte[] buff, int buffOffset, int len)
	    throws IOException{
	  this.write(mhlc, mhlc.r, blockOffset, buff, buffOffset, len);
	}
	
	/**
	 * @param mhlc
	 * @param userTimestamp
     * @param blockOffset
	 * @param buff
	 * @param buffOffset
	 * @param len
	 * @throws IOException
	 */
	public abstract void write(HybridLogicalClock mhlc, long userTimestamp, long blockOffset, byte[] buff, int buffOffset, int len) throws IOException;
}
