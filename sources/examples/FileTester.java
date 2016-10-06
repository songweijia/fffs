import java.io.PrintStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import edu.cornell.cs.blog.JNIBlog;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;

public class FileTester extends Configured implements Tool {
  public static void main(String[] args){
    int res;
    try{
      res = ToolRunner.run(new Configuration(), new FileTester(), args);
      System.exit(res);
    }catch(Exception e){
      System.out.println(e);
      e.printStackTrace();
    }
  }
  
  void overwriteFile(FileSystem fs, String path, String... args)
  throws Exception{
    if(args.length != 2){
      throw new IOException("Please specify the pos and data");
    }
    FSDataOutputStream fsos = fs.append(new Path(path));
    fsos.seek(Long.parseLong(args[0]));
    System.out.println("seek done");
    PrintStream ps = new PrintStream(fsos);
    ps.print(args[1]);
    System.out.println("write done");
    ps.close();
    fsos.close();
    System.out.println("close done");
  }

  void appendFile(FileSystem fs, String path, String... args)
  throws IOException{
    if(args.length != 1){
      throw new IOException("Please specify the data");
    }
    FSDataOutputStream fsos = fs.append(new Path(path));
    PrintStream ps = new PrintStream(fsos);
    ps.print(args[0]);
    ps.close();
    fsos.close();
  }

  void timeAppend(FileSystem fs, String path, String... args)
  throws IOException{

    if(args.length != 2)
      throw new IOException("Please specify the write length and duration in second");

    int wsize = Integer.parseInt(args[0]);
    long dur = Long.parseLong(args[1]);
    long nCnt = 0;
    byte buf[] = new byte[wsize];

    for(int i=0;i<wsize;i++)buf[i]=(byte)'X';

    FSDataOutputStream fsos = fs.append(new Path(path));
    long t_start = System.nanoTime();
    long t_end = t_start;
    while(t_end - t_start < dur*1000000000L){
      fsos.write(buf);
      t_end = System.nanoTime();
      nCnt ++;
    }
    fsos.flush();
    long nanoDur = System.nanoTime() - t_start;

    System.out.println(String.format("%1$.3f",((double)nCnt*wsize/(t_end - t_start)))+" GB/s");

    fsos.close();
  }

  void write(FileSystem fs, String path,int filesizeMB, int bfsz)
  throws IOException{
    FSDataOutputStream fsos = fs.create(new Path(path));
    byte [] buf = new byte[bfsz];
    int i;
    for(i=0;i<bfsz;i++)buf[i]=(byte)'a';
    int nloop = (int)((filesizeMB*(1l<<20))/bfsz);
    int logMod = (1<<26)/bfsz;
    long []tsarr = new long[nloop/logMod + 1];
    int pos=0;
    for(i=0;i<nloop;i++){
      if(i%logMod == 0)
        tsarr[pos++] = System.currentTimeMillis();
      fsos.write(buf,0,bfsz);
    }
    fsos.close();

    //print throughput
    for(i=1;i<pos;i++){
      System.out.println((tsarr[i]+tsarr[i-1])/2 + "\t" + 
        ((double)(1L<<26))/(tsarr[i]-tsarr[i-1])/1000 + "\t" + "MB/s");
    }
  }

  // Note: please set the block size to 1MB
  void randomWrite(FileSystem fs, String path)
  throws IOException{
/*    Path p = new Path(path);
    byte [] buf = new byte[4096];
    int i;
    // 0) Initialize write 4KB for 1024 times.
    for(i=0;i<4096;i++)buf[i]='I';
    FSDataOutputStream fsos = fs.create(new Path(path));
    for(i=0;i<1024;i++)fsos.write(buf,0,4096);
//    fsos.close();
    // 1) write 4K at 0; 4K at 1044480; 4K at 100000
    for(i=0;i<4096;i++)buf[i]='1';
    fsos.seek(0);fsos.write(buf,0,4096);
    fsos.seek(1044480);fsos.write(buf,0,4096);
    fsos.seek(100000);fsos.write(buf,0,4096);
//    fsos.close();
    // 2) write cross blocks
    // from 1048000 -> 1049000
    for(i=0;i<4096;i++)buf[i]='2';
    fsos.seek(1048000);fsos.write(buf,0,1000);
    // from 2097000 to 3146000
    fsos.seek(2097000);
    for(int j=0;j<1049;j++)fsos.write(buf,0,1000);
    fsos.close();
*/  }

  void snapshot(FileSystem fs,String path, long msInt, int count)
  throws IOException{
    long st = System.currentTimeMillis();//JNIBlog.readLocalRTC();
    int curc = 0;
    while(curc < count){
      long sts = System.nanoTime();
      fs.createSnapshot(new Path(path),""+curc);
      long ets = System.nanoTime();
      System.out.println((double)(ets-sts)/1000000);
      curc++;
      while(/*JNIBlog.readLocalRTC()*/System.currentTimeMillis()<(st+curc*msInt)){
        try{Thread.sleep(1);}catch(InterruptedException ie){}
      }
    }
  }

  void zeroCopyRead(FileSystem fs,String path,
    int readSize,int nloop) throws IOException{

    long start_ts,end_ts,len=0;

    ByteBuffer bb = ByteBuffer.allocate(readSize);
    HdfsDataInputStream fsis = (HdfsDataInputStream)fs.open(new Path(path));
    for(int i=0;i<nloop;i++){
      fsis.seek(0);
      len=0;
      long ts1,ts2;
      start_ts = System.nanoTime();
      while(true){
	bb = fsis.rdmaRead(readSize);
        if(bb==null)break;
        len += bb.remaining();
      };
      end_ts = System.nanoTime();
      System.out.println(((double)len/(end_ts - start_ts))+" GB/s");
    }
    fsis.close();
  }

  void standardRead(FileSystem fs,String path,
    int readSize,int nloop)
  throws IOException{
    long start_ts,end_ts,len=0;
    int nRead;
    ByteBuffer bb = ByteBuffer.allocate(readSize);
    FSDataInputStream fsis = fs.open(new Path(path));
    for(int i=0;i<nloop;i++){
      fsis.seek(0);
      len=0;
      long ts1,ts2;
      start_ts = System.nanoTime();
      while(true){
        nRead=fsis.read(bb);
        if(nRead <= 0)break;
        len+=nRead;
        bb.clear();
      };
      end_ts = System.nanoTime();
      System.out.println(String.format("%1$.3f",((double)len/(end_ts - start_ts)))+" GB/s");
    }
    fsis.close();
  }

  void syncMultiRead(final FileSystem fs, final String path, final long beginTime,
    final int readSize, final int nThread, final boolean bZeroCopy) throws IOException{

    Thread thrds[] = new Thread[nThread];
    int thd;

    // create threads
    for(thd=0;thd<nThread;thd++){
      final int cno = thd;
      thrds[thd] = new Thread(new Runnable(){
        @Override
        public void run(){
          long start_ts,end_ts,len=0;

          try{

            // open file
            FSDataInputStream fsis = fs.open(new Path(path+"."+cno));
            byte [] buf = new byte[readSize];

            // wait for time to read
            while(System.currentTimeMillis() < beginTime){
              try{
                Thread.sleep(100);
              }catch(InterruptedException ie){
                //do nothing
              }
            };

            // write 
            long bytesRead = 0L;
            HdfsDataInputStream hdis = (HdfsDataInputStream)fsis;
            start_ts = System.nanoTime();
            while(true){
              if(bZeroCopy){
                ByteBuffer bb = hdis.rdmaRead(readSize);
                if(bb==null)
                  break;
              }
              else{
                int nRead = fsis.read(buf,0,readSize);
                if(nRead==-1)
                  break;
                else
                  bytesRead += nRead;
              }
            }
            end_ts = System.nanoTime();
            fsis.close();

            // print
            System.out.println( (end_ts-start_ts) + " ns" );
          } catch (IOException ioe) {
            System.out.println("Exception:"+ioe);
            ioe.printStackTrace();
          }

        }
      });
      thrds[thd].start();
    }

    // waiting for threads
    for(thd=0;thd<nThread;thd++){
      try{
        thrds[thd].join();
      }catch(InterruptedException ie){
        System.err.println("syncMultiRead() fail to join thread-"+thd);
      }
    }
  }


  void syncMultiWrite(final FileSystem fs, final String path, final long beginTime,
    final long fsize, final int writeSize, final int nThread) throws IOException{

    Thread thrds[] = new Thread[nThread];
    int thd;

    // create threads
    for(thd=0;thd<nThread;thd++){
      final int cno = thd;
      thrds[thd] = new Thread(new Runnable(){
        @Override
        public void run(){
          long start_ts,end_ts,len=0;

          try{

            // open file
            FSDataOutputStream fsos = fs.create(new Path(path+"."+cno));
            byte [] buf = new byte[writeSize];
            for(int i=0;i<writeSize;i++)buf[i]=(byte)'a';

            // wait for time to write
            while(System.currentTimeMillis() < beginTime){
              try{
                Thread.sleep(1);
              }catch(InterruptedException ie){
                //do nothing
              }
            };

            // write 
            long bytesWritten = 0L;
            start_ts = System.nanoTime();
            while(bytesWritten < fsize){
              fsos.write(buf,0,writeSize);
              bytesWritten += writeSize;
            }
            fsos.hflush();
            fsos.hsync();
            fsos.close();

            // print
            end_ts = System.nanoTime();
            System.out.println( (end_ts-start_ts) + " ns" );
          } catch (IOException ioe) {
            System.out.println("Exception:"+ioe);
            ioe.printStackTrace();
          }

        }
      });
      thrds[thd].start();
    }

    // waiting for threads
    for(thd=0;thd<nThread;thd++){
      try{
        thrds[thd].join();
      }catch(InterruptedException ie){
        System.err.println("syncMultiWrite() fail to join thread-"+thd);
      }
    }
  }

  void addTimestamp(long ts,byte[] buf){
    StringBuffer sb = new StringBuffer();
    sb.append(ts);
    sb.append(" ");
    byte bs[] = sb.toString().getBytes();
    System.arraycopy(bs,0,buf,0,bs.length);
  }

  void timewrite(FileSystem fs,String path,int bufsz, int dur)
  throws IOException{
    byte buf[] = new byte[bufsz];
    for(int i=0;i<bufsz;i++)buf[i]='P';
    buf[bufsz-1]='\n';
    FSDataOutputStream fsos = fs.create(new Path(path));
    long end = System.currentTimeMillis()+dur*1000;
    long cur = System.currentTimeMillis()+33;//JNIBlog.readLocalRTC()+33;
    while(System.currentTimeMillis() < end){
      //write a packet
      if(/*JNIBlog.readLocalRTC()*/ System.currentTimeMillis() >= cur){
        addTimestamp(cur,buf);
        fsos.write(buf,0,bufsz);
        fsos.hflush();
        cur += 33;
      }
    }
    fsos.close();
  }

  void pmuwrite(FileSystem fs,String path,
    int pmuid, int recordsize, int dur)
  throws IOException{
    byte buf[] = new byte[recordsize];
    for(int i=0;i<recordsize;i++)buf[i]='P';
    buf[recordsize-1]='\n';
    FSDataOutputStream fsos = fs.create(new Path(path+"/pmu"+pmuid));
    long end = System.currentTimeMillis()+dur*1000;
    long cur = System.currentTimeMillis()+33;//JNIBlog.readLocalRTC()+33;
    while(System.currentTimeMillis() < end){
      //write a packet
      if(/*JNIBlog.readLocalRTC()*/System.currentTimeMillis() >= cur){
        addTimestamp(cur,buf);
        fsos.write(buf,0,recordsize);
        fsos.hflush();
        cur += 33;
      }
    }
    fsos.close();
  }

  private List<Long> getSnapshots(FileSystem fs)
  throws IOException{
    List<Long> lRet = new Vector<Long>(256);
    for(FileStatus stat: fs.listStatus(new Path("/.snapshot"))){
      Long snapts = Long.parseLong(stat.getPath().getName());
      lRet.add(snapts);
    }
    return lRet;
  }

  class FileTuple{
    String name; // file name
    long sts; // start time stamp
    long ets; // end time stamp
  }

  private long readFirstTs(FileSystem fs,Path path)
  throws IOException{
    FSDataInputStream fsis = fs.open(path);
    long lRet = -1;
    if(fsis.available()>13){
      byte buf[] = new byte[13];
      fsis.readFully(0,buf);
      lRet = Long.parseLong(new String(buf));
    }
    fsis.close();
    return lRet;
  }

  private long readLastTs(FileSystem fs,Path path)
  throws IOException{
    long lRet = -1;
    if(!fs.exists(path)) return -1;
    FSDataInputStream fsis = null;
    try{
      fsis = fs.open(path);
      int back = 64;
      int flen = fsis.available();
      if(flen > 0)
        while(true){
          int try_pos = Math.max(flen - back, 0);
          byte buf[] = new byte[flen - try_pos];
          fsis.readFully(try_pos,buf);
          if(buf[buf.length-1]=='\n')
            buf[buf.length-1]='P';
          String line = new String(buf);
          int pts = line.lastIndexOf('\n');
          if(pts == -1 && try_pos != 0)continue;
          if(pts != -1 && pts+14 <= line.length())
            lRet = Long.parseLong(line.substring(pts+1,pts+14));
           break;
        }
    }catch(IOException ioe){
      return -1;
    }finally{
      if(fsis!=null)fsis.close();
    }
    return lRet;
  }

  private List<FileTuple> getFiles(FileSystem fs, String path)
  throws IOException{
    List<FileTuple> lRet = new Vector<FileTuple>(32);
    for(FileStatus stat: fs.listStatus(new Path(path))){
      FileTuple ft = new FileTuple();
      ft.name = stat.getPath().getName();
      ft.sts = readFirstTs(fs,stat.getPath());
      ft.ets = readLastTs(fs,stat.getPath());
      lRet.add(ft);
    }
    return lRet;
  }

  void analyzesnap(FileSystem fs, String path)
  throws IOException{
    // STEP 1: get snapshot/timestamp list
    List<Long> lSnap = getSnapshots(fs);
    // STEP 2: get the real start/end timestamp for each file.
    List<FileTuple> lFile = getFiles(fs,path);
    // STEP 3: spit data
    for(long snap: lSnap){
      for(FileTuple ft: lFile){
        if(ft.sts > snap)continue;
        Path p = new Path("/.snapshot/"+snap+path+"/"+ft.name);
        long delta = snap - ft.sts;
        long ets = readLastTs(fs,p);

        if(ets!=-1){
          delta = snap - ets;
          if(snap > ft.ets){
            delta = ft.ets - ets;
            if(delta <= 0) continue;
          }
        }

        System.out.println(ft.name+" "+snap+" "+delta);
      }
    }
  }

  void readto(FileSystem fs, String srcpath, String destpath, long timestamp, boolean bUserTimestamp)
  throws IOException{
    System.out.println("user timestamp="+bUserTimestamp);
    //STEP 1 open source file.
    FSDataInputStream fin = fs.open(new Path(srcpath),4096,timestamp,bUserTimestamp);
    //STEP 2 open destination file.
    FileOutputStream fos = new FileOutputStream(destpath);
    //STEP 3 copy.
    byte buf[] = new byte[4096];
    do{
      int nRead;
      nRead = fin.read(buf);
      if(nRead < 0)break;
      fos.write(buf,0,nRead);
    }while(true);
    //STEP 4 done.
    fos.close();
    fin.close();
  }

  void write_ts64(FileSystem fs, String path, int ncount) throws Exception{
    //STEP 1 create file.
    FSDataOutputStream os = fs.create(new Path(path+".ts64"));
    //STEP 2 write numbers.
    byte buf[] = new byte[8];
    for(int i=0;i<ncount;i++){
      buf[7]++;
      os.write(buf,0,8);
      os.hflush();
    }
    os.close();
  }

  void snapwrite(FileSystem fs, String path, //filename
        int fsizemb,//filesize(MB)
        int wsize,//writesize(Byte)
        int icount,//interval count
        int imillis,//interval millis
        int scount//snap count
        ) throws Exception{
    byte [] wbuf = new byte[wsize];
    int i,j,k;
    final int filesize = fsizemb<<20;
    Random rand = new Random(System.nanoTime());

    // STEP 0 write wsize with random numbers.
    rand.nextBytes(wbuf);

    // STEP 1 create file.
    FSDataOutputStream fsos = fs.create(new Path(path));
    System.out.println(path);

    // STEP 2 write initial data.
    for(i=filesize;i>0;i-=wsize){
      fsos.write(wbuf);
    }
    fsos.hflush();
    fsos.close();

    // STEP 3 begin random write
    fsos = fs.append(new Path(path));
    for(i=0; i<scount; i++){
      // STEP 3.1 write a time stamp
      long ts = System.currentTimeMillis();
      System.out.println(ts);
      // STEP 3.2 do random write
      for(j=0; j<icount; j++){
        int pos = rand.nextInt(filesize - wsize);
        fsos.seek(pos);
        fsos.write(wbuf);
      }
      // STEP 3.3 do wait...
      while((System.currentTimeMillis()-ts) < imillis){
        try{
          Thread.sleep(1);
        } catch (InterruptedException ie){
          //do nothing
        }
      }
    }
    fsos.close();
  }

  static class SnapLog{
    String path;
    Vector<Long> snapshots;
    static SnapLog load(String snapfile)throws IOException{
      SnapLog sl = new SnapLog();
      // STEP 1: open file
      FileReader fr = new FileReader(snapfile);
      BufferedReader br = new BufferedReader(fr);
      sl.path = br.readLine();
      sl.snapshots = new Vector<Long>();
      while(true){
        String l = br.readLine();
        if(l==null)
          break;
        sl.snapshots.add(Long.parseLong(l));
      }
      fr.close();
      return sl;
    }
  }

  void snapread(FileSystem fs, String path, boolean bReverse)
  throws Exception{
    // STEP 1: parse snaplog file:
    SnapLog sl = SnapLog.load(path);
    // Line 1: full path to the file
    int nr_snap = sl.snapshots.size();
    // Rest lines: snapshot time
    for(int i=(bReverse?nr_snap-1:0);
        bReverse?(i>=0):(i<nr_snap);
        i+=(bReverse?-1:1)){

      FSDataInputStream fin = fs.open(new Path(sl.path),131072,sl.snapshots.get(i),false);
      byte buf[] = new byte[131072];
      long ts = System.nanoTime();
      do{
        int nRead;
        nRead = fin.read(buf);
        if(nRead < 0)break;
      }while(true);
      long te = System.nanoTime();

      fin.close();
      System.out.println(i+"\t"+(te-ts)/1000+"."+(te-ts)%1000+" us");
    }
  }

  @Override
  public int run(String[] args) throws Exception{
    Configuration conf = this.getConf();
    conf.set("fs.defaultFS", "hdfs://128.84.105.178:50090");
    FileSystem fs = FileSystem.get(conf);
    System.out.println(fs.getUri().toString());

    if(args.length < 1){
      System.out.println("args: <cmd:=append|overwrite>\n"+
        "\tappend <file> <data>\n"+
        "\ttimeappend <file> <ws> <dur>\n"+
        "\toverwrite <file> <pos> <data>\n"+
        "\twrite <file> <size(MB)> <bfsz(B)>\n"+ //set buffersize
        "\trandomwrite <file>\n"+ 
        "\tsnapshot <path> <interval_ms> <number>\n"+
        "\ttimewrite <path> <bfsz> <duration>\n"+
        "\tpmuwrite <path> <pmuid> <recordsize> <duration>\n"+
        "\tzeroCopyRead <path> <readSize> <nloop>\n"+
        "\tstandardRead <path> <readSize> <nloop>\n"+
        "\tanalyzesnap <path>\n"+
        "\treadto <srcpath> <destpath> <timestamp> <bUserTimestamp>\n"+
        "\twrite.ts64 <filepath> <ncount>\n" +
        "\tsyncmultiwrite <filepath> <begintime(sec since 1970-01-01)> <filesize> <writesize> <numthread>\n" +
        "\tsyncmultiread <filepath> <begintime(sec since 1970-01-01)> <readsize> <numthread>\n" +
        "\tsyncmultizerocopyread <filepath> <begintime(sec since 1970-01-01)> <readsize> <numthread>\n" +
        "\tsnapwrite <filename> <filesize(MB)> <writesize(Byte)> <interval(cnt)> <interval(ms)> <snapcount>\n" +
        "\tsnapread <snaplog> <reverse:True|False>\n" +
        "\tr120");
      return -1;
    }
    if("append".equals(args[0]))
      this.appendFile(fs,args[1],args[2]);
    else if("timeappend".equals(args[0]))
      this.timeAppend(fs,args[1],args[2],args[3]);
    else if("overwrite".equals(args[0]))
      this.overwriteFile(fs,args[1],args[2],args[3]);
    else if("write".equals(args[0]))
      this.write(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    else if("randomwrite".equals(args[0]))
      this.randomWrite(fs,args[1]);
    else if("snapshot".equals(args[0]))
      this.snapshot(fs,args[1],Long.parseLong(args[2]),Integer.parseInt(args[3]));
    else if("standardRead".equals(args[0]))
      this.standardRead(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    else if("zeroCopyRead".equals(args[0]))
      this.zeroCopyRead(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    else if("timewrite".equals(args[0]))
      this.timewrite(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    else if("pmuwrite".equals(args[0]))
      this.pmuwrite(fs,args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]),Integer.parseInt(args[4]));
    else if("analyzesnap".equals(args[0]))
      this.analyzesnap(fs,args[1]);
    else if("readto".equals(args[0]))
      this.readto(fs,args[1],args[2],Long.parseLong(args[3]),Boolean.parseBoolean(args[4]));
    else if("write.ts64".equals(args[0]))
      this.write_ts64(fs,args[1],Integer.parseInt(args[2]));
    else if("syncmultiwrite".equals(args[0]))
      this.syncMultiWrite(fs,args[1],Long.parseLong(args[2])*1000,
        Long.parseLong(args[3]),Integer.parseInt(args[4]),Integer.parseInt(args[5]));
    else if("syncmultiread".equals(args[0]))
      this.syncMultiRead(fs,args[1],Long.parseLong(args[2])*1000,
        Integer.parseInt(args[3]),Integer.parseInt(args[4]),false/*standard copy*/);
    else if("syncmultizerocopyread".equals(args[0]))
      this.syncMultiRead(fs,args[1],Long.parseLong(args[2])*1000,
        Integer.parseInt(args[3]),Integer.parseInt(args[4]),true/*zero copy*/);
    else if("snapwrite".equals(args[0]))
      this.snapwrite(fs,args[1],  //filename
        Integer.parseInt(args[2]),//filesize(MB)
        Integer.parseInt(args[3]),//writesize(Byte)
        Integer.parseInt(args[4]),//interval count
        Integer.parseInt(args[5]),//interval millis
        Integer.parseInt(args[6])//snap count
        );
    else if("snapread".equals(args[0]))
      this.snapread(fs,args[1], //logfile
        Boolean.parseBoolean(args[2])                //reverse or not
      );
    else
      throw new Exception("invalid command:"+args[0]);
    return 0;
  }
}
