#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include "InfiniBandRDMA.h"

#define TEST_NZ(x,y) do { if ((x)) die(y); } while (0)
#define TEST_Z(x,y) do { if (!(x)) die(y); } while (0)
#define TEST_N(x,y) do { if ((x)<0) die(y); } while (0)

#define DEFAULT_PORT (18515)
static int die(const char *reason){                                                                     
  fprintf(stderr, "Err: %s - %s \n ", strerror(errno), reason);
  exit(EXIT_FAILURE);
  return -1;
}

const char * HELP_INFO = " \
Client: test -c -h <server> -p <port> -d <dev> \
[-z <poolsize order>] [-a <pagesize order>] \n \
Server: test -s -p <port> -d <dev> \
[-z <poolsize order>] [-a <pagesize order>] [-l <loop count>]  \n \
";

static void runClient(
  const char * host,
  const uint16_t port, 
  const char *dev,
  const uint32_t pool_size_order,
  const uint32_t page_size_order);

static void runServer(
  const uint16_t port, 
  const char *dev,
  const uint32_t pool_size_order,
  const uint32_t page_size_order);

// USAGE:
// test -c -h host -p port -d mlx5_0
// test -s -p port -d mlx5_1
// options:
// -z poolsize order, default is 20 for 1MB
// -a page/buffer size order, default value is 12 for 4KB
// -n loop count
int main(int argc, char **argv){
  int c;
  int mode = -1; // 0 - client; 1 - server
  char *host=NULL, *dev=NULL;
  uint16_t port=DEFAULT_PORT;
  uint32_t psz=20;// the default pool size is 1MB.
  uint32_t align=12;// the default page or buffer size is 4KB.
  while((c=getopt(argc,argv,"cs:h:p:a:z:d:"))!=-1){
    switch(c){
    case 'c':
      if(mode!=-1){
        fprintf(stderr,"Please only specify -c or -s once.\n");
        return -1;
      }
      mode = 0;
      break;
    case 's':
      if(mode!=-1){
        fprintf(stderr,"Please only specify -c or -s once.\n");
        return -1;
      }
      mode = 1;
      break;
    case 'h':
      host = optarg;
      break;
    case 'd':
      dev = optarg;
      break;
    case 'p':
      port = (unsigned short)atoi(optarg);
      break;
    case 'z':
      psz = (uint32_t)atoi(optarg);
      break;
    case 'a':
      align = (uint32_t)atoi(optarg);
      break;
    }
  }
  printf("mode=%d,host=%s,dev=%s,port=%d,psz=%d,align=%d\n",mode,host,dev,port,psz,align);
  if(mode == 0){
    runClient(host,port,dev,psz,align);
  }else if(mode == 1){
    runServer(port,dev,psz,align);
  }else{
    fprintf(stderr,"%s",HELP_INFO);
    return -1;
  }
  return 0;
}


static void runClient(
  const char * host,
  const uint16_t port, 
  const char *dev,
  const uint32_t pool_size_order,
  const uint32_t page_size_order){

  RDMACtxt rdma_ctxt;
  uint64_t i;
  // step 1: initialize client
  TEST_NZ(initializeContext(&rdma_ctxt,NULL,pool_size_order,page_size_order,dev,port,1),"initializeContext");
  for(i=0;i<(1lu<<(pool_size_order-page_size_order));i++)
    memset((void*)((char*)rdma_ctxt.pool+(i<<page_size_order)),'A'+i,1l<<page_size_order);

  // step 2: connect
  printf("before calling rdmaConnect\n");
  fflush(stdout);
  TEST_NZ(rdmaConnect(&rdma_ctxt,inet_addr(host)),"rdmaConnect");
  printf("after calling rdmaConnect\n");
  fflush(stdout);
  // step 3: wait for RDMA test
  while(1){
    getchar();
    break;
  }

  return;
}

static void runServer(
  const uint16_t port, 
  const char *dev,
  const uint32_t pool_size_order,
  const uint32_t page_size_order){

  RDMACtxt rdma_ctxt;

  // step 1: initialize server
  TEST_NZ(initializeContext(&rdma_ctxt,NULL,pool_size_order,page_size_order,dev,port,0),"initializeContext");

    // step 2: test
  while(1){
    // int pns[4];
    // uint64_t len, *ids;
    uint64_t rvaddr;
    uint64_t cipkey;
    // int i;
    int ret,nloop;
    uint64_t j;
    RDMAConnection *conn;
    printf("please give the client info(like:<ipkey>1c09a8c0000078e8 <vaddr>70fffffffff <loop>1024):\n");
    ret = scanf("%lx %lx %d",&cipkey,&rvaddr,&nloop);
    printf("cipkey=0x%lx\n",cipkey);
    printf("vaddr=0x:%lx\n",rvaddr);
    printf("loop count=%d\n",nloop);

    // step 3: do transfer
    if(MAP_READ(con,rdma_ctxt.con_map,(uint64_t)cipkey,&conn) != 0){
      fprintf(stderr, "cannot get connection %lx from connection map.\n",cipkey);
      return;
    }

    uint64_t npages = 1l<<(pool_size_order - page_size_order);
    const void **pagelist = (const void **)malloc(sizeof(void*)*npages);

    for(j=0;j<npages;j++)
    {
      pagelist[j] = (const void *)((char*)conn->l_vaddr+(j<<page_size_order));
    }
    
    while(nloop-- > 0){
      fprintf(stdout,"loop %d\n",nloop);
      // step 4: test rdmaWrite:
      struct timeval tv1,tv2;
      uint64_t rus,wus;
      gettimeofday(&tv1,NULL);
      ret = rdmaWrite(&rdma_ctxt,(uint32_t)(cipkey>>32),(uint32_t)(cipkey&0xffffffff),rvaddr,pagelist,npages,1<<page_size_order);
      gettimeofday(&tv2,NULL);
      wus = (tv2.tv_sec-tv1.tv_sec)*1000000l + tv2.tv_usec-tv1.tv_usec;
      if(ret != 0)
        fprintf(stdout, "RDMA write failed with error:%s!\n",strerror(errno));

      // step 5: test rdmaRead:
      gettimeofday(&tv1,NULL);
      ret = rdmaRead(&rdma_ctxt,(uint32_t)(cipkey>>32),(uint32_t)(cipkey&0xffffffff),rvaddr,pagelist,npages,1<<page_size_order);
      gettimeofday(&tv2,NULL);
      rus = (tv2.tv_sec-tv1.tv_sec)*1000000l + tv2.tv_usec-tv1.tv_usec;
      if(ret != 0)
        fprintf(stdout, "RDMA read failed with error:%s!\n",strerror(errno));

      // step 5: wait for data being transfered.
      fprintf(stdout, "Write: %.3fGbps, Read: %.3fGbps.\n",
        (double)(1l<<pool_size_order)*8/wus/1000.0,
        (double)(1l<<pool_size_order)*8/rus/1000.0
      );
    }
  }
}
