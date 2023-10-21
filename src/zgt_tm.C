/**
 * Group #9
 * Implementor: Aman Hogan-Bailey
 * UTAID - 1001830469
 * Instructor: Professor Ashraf Aboulnaga
 * Section: 2238-CSE-5331-002-DBMS MODELS AND IMPLEMENTATION
*/


/* the Tx mgr functions are implemented here */
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <sys/sem.h>
#include <fstream>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"

#define TEAM_NO 9

/**
 * @brief Sets the value of member function logfile and opens the file for writing in the append mode.
 * @param lfile filename of the log file
 * @returns None
*/
void zgt_tm::openlog(string lfile)
{
   #ifdef TM_DEBUG
      printf("entering openlog\n");fflush(stdout);
   #endif
   
   this->logfilename = (char *) malloc(sizeof(char) * MAX_FILENAME);
      
   int i = 0;
   while(lfile[i] !='\0')
   {
      this->logfilename[i] = lfile[i];
      i++;
   } 
   
   logfilename[--i] = '\0';
   
   #ifdef TM_DEBUG
      printf("\nGiven log file name: %s\n", logfilename);fflush(stdout);
   #endif

   if ((this->logfile = fopen(this->logfilename, "w")) == NULL)
   {
      printf("\nCannot open log file for write/append:%s\n", logfilename);fflush(stdout);
      exit(1);
   }

   fprintf(this->logfile, "---------------------------------------------------------------------------\n");
   fprintf(this->logfile, "TxId\tTxtype\tOperation\tObId:Obvalue:optime\tLockType\tStatus\t\tTxStatus\n");
   fflush(this->logfile);
   
   #ifdef TM_DEBUG
      printf("leaving openlog\n");fflush(stdout);
      fflush(stdout);
   #endif
}

/**
 * @desc Creates a thread and calls the constructor of the Transaction class
 * Creates object and initializes the other members of the Transaction class using the
 * function begintx(void *thdarg). To pass arguments, you pass in the thread args as a struct.
 * @param tid Transaction id (Ex: Tx1, Tx2, etc)
 * @param thrNum Unique thread id
 * @param type Type of Transaction, e.g Read or Write
 * @return an integer, 0 for success, -1 for failure
*/
int zgt_tm::BeginTx(long tid, int thrNum, char type)
{
   #ifdef TM_DEBUG
      printf("\nCreating BeginTx thread for Tx: %ld\n", tid);
      fflush(stdout);
   #endif

   struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
   pthread_t thread1;
   nodeinfo->tid = tid;
   nodeinfo->Txtype = type;
   nodeinfo->obno = -1;
   nodeinfo->count= SEQNUM[tid]=0;

   int status;
   
   status = pthread_create(&threadid[thrNum], NULL, begintx,(void*)nodeinfo); // Creates new thread that calls begintx
   
   if (status)
   {
      printf("ERROR: return code from pthread_create() is:%d\n", status);
      exit(-1);
   }

   #ifdef TM_DEBUG
      printf("\nfinished creating BeginTx thread for Tx: %ld\n", tid);
      fflush(stdout);
   #endif
      
   return(0);
}     

/**
 * @desc Sets tx manager semphaore, creates a thread and checks if the thread 
 * can proceed based on the consdition (Prevents 2 operations from the same tx follow one another).
 * Then gets lock to perform a read operation (Prints value)
 * Then Write operation increments value by one.
 * Creates theread and calls readtx(void *)
 * @param tid Transaction id (Ex: Tx1, Tx2, etc)
 * @param thrNum Unique thread id
 * @param obno Unique object number
 * 
*/
int zgt_tm::TxRead(long tid, long obno, int thrNum)
{
   #ifdef TM_DEBUG
      printf("\ncreating TxRead thread for Tx: %ld\n", tid);
      fflush(stdout);
   #endif
   
   pthread_t thread1;
   struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
   nodeinfo->tid = tid;
   nodeinfo->obno = obno;
   nodeinfo->Txtype = ' ';
   nodeinfo->count = --SEQNUM[tid];

   int status;
   status = pthread_create(&threadid[thrNum],NULL,readtx,(void*)nodeinfo);
   
   if (status)
   {
      printf("ERROR: return code from pthread_create() is:%d\n", status);
      exit(-1);
   }

   #ifdef TM_DEBUG
      printf("\nexiting TxRead thread create for Tx: %ld\n", tid);
      fflush(stdout);
   #endif

   return(0);
}

int zgt_tm::TxWrite(long tid, long obno, int thrNum)
{
  #ifdef TM_DEBUG
      printf("\nCreating TxWrite thread for Tx: %ld\n", tid);
      fflush(stdout);
   #endif
   
   pthread_t thread1;

   struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
   nodeinfo->tid = tid;
   nodeinfo->obno = obno;
   nodeinfo->Txtype = ' ';
   nodeinfo->count = --SEQNUM[tid];

   int status;
   status = pthread_create(&threadid[thrNum],NULL,writetx,(void*)nodeinfo);
   
   if (status)
   {
      printf("ERROR: return code from pthread_create() is:%d\n", status);
      exit(-1);
   }

   #ifdef TM_DEBUG
      printf("\nExiting TxWrite thread create for Tx: %ld\n", tid);
      fflush(stdout);
   #endif

   return(0);
}

int zgt_tm::CommitTx(long tid, int thrNum)
{
   #ifdef TM_DEBUG
      printf("\nCreating Commit thread for Tx: %ld\n", tid);
      fflush(stdout);
   #endif
   
   pthread_t thread1;
   struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
   nodeinfo->tid = tid;
   nodeinfo->obno = -1;
   nodeinfo->Txtype = ' ';
   nodeinfo->count = --SEQNUM[tid];

   int status;
   status = pthread_create(&threadid[thrNum],NULL,committx,(void*)nodeinfo);
   
   if (status)
   {
      printf("ERROR: return code from pthread_create() is:%d\n", status);
      exit(-1);
   }

   #ifdef TM_DEBUG
      printf("\nexiting TxCommit thread create for Tx: %ld\n", tid);
      fflush(stdout);
   #endif

   return(0);
}
 
int zgt_tm::AbortTx(long tid, int thrNum)
{       
   #ifdef TM_DEBUG
      printf("\nCreating Abort thread for Tx: %ld\n", tid);
      fflush(stdout);
   #endif
   
   pthread_t thread1;
   struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
   nodeinfo->tid = tid;
   nodeinfo->obno = -1;
   nodeinfo->Txtype = ' ';
   nodeinfo->count = --SEQNUM[tid];

   int status;
   status = pthread_create(&threadid[thrNum],NULL,aborttx,(void*)nodeinfo);
   
   if (status)
   {
      printf("ERROR: return code from pthread_create() is:%d\n", status);
      exit(-1);
   }

   #ifdef TM_DEBUG
      printf("\nExiting TxAbort thread create for Tx: %ld\n", tid);
      fflush(stdout);
   #endif

   return(0);
}

int zgt_tm::endTm(int thrNum)
{
   int rc=0;
   int i;

   #ifdef TM_DEBUG
      printf("\nEntering End of schedule thread with thrNum: %d\n", thrNum);
      fflush(stdout);
   #endif
   
   printf("Wait for threads and cleanup\n");
   for (i=0; i < thrNum; i++) 
   {
      rc = pthread_join(threadid[i], NULL);
      printf("Thread %d completed with ret value: %d\n", i, rc);
      fflush(stdout);
   }

   printf("ALL threads finished their work\n");
   fflush(stdout);
   printf("Releasing mutexes and condpool\n");
   fflush(stdout);

   // release condpool and mutexpool
   for(i=1;i<MAX_TRANSACTIONS+1;++i)
   {
      pthread_mutex_destroy(&mutexpool[i]);
      pthread_cond_destroy(&condpool[i]);
   }

   printf("Relasing all semaphores\n"); 
   fflush(stdout);
   zgt_sem_release(ZGT_Semid);
   printf("endTm completed\n");
   fflush(stdout);

   #ifdef TM_DEBUG
      printf("\nFinished end of schedule thread: endTm\n");
      fflush(stdout);
   #endif

   fclose(this->logfile);
   return(0);

 }
 
//Not used for project 2 in 2023
//This routine should detect a deadlock and print the cycles involved in the deadlock to 
//output  and log. In order to do that, spawn a thread that acquires the tm lock and 
//constructs the wait for graph. You can use the data structure given in ddlock.h or 
//modify it suitably 
int zgt_tm::ddlockDet()
{       
   #ifdef TM_DEBUG
      printf("\ncreating ddlockDet thread\n");
      fflush(stdout);
   #endif

   pthread_t thread1;
   int  status;

   #ifdef TM_DEBUG
      printf("\nleaving ddlockDet thread create\n");
      fflush(stdout);
   #endif

   return(0);  //successful operation
}
 
//Not used for project 2
//This routine should detect a deadlock and print the cycles involved in the deadlock to 
//output  and log. In order to do that, spawn a thread that acquires the tm lock and 
//constructs the wait for graph. You can use the data structure given in ddlock.h or 
//modify it suitably 
int zgt_tm::chooseVictim()
{       
   #ifdef TM_DEBUG
      printf("\ncreating chooseVictim thread\n");
      fflush(stdout);
   #endif

   pthread_t thread1;
   int  status;
   //   status=pthread_create(&thread1, NULL,choosevictim,(void*)NULL);	// Fork a thread and pass tid
   //if (status){
   //printf("ERROR: return code from pthread_create() is:%d\n", status);
   //exit(-1);
   //}
   #ifdef TM_DEBUG
      printf("\nleaving chooseVictim creating thread\n");
      fflush(stdout);
   #endif

   return(0);
}

//important; understand this
zgt_tm::zgt_tm()
{

   #ifdef TM_DEBUG
      printf("\nInitializing the TM\n");
      fflush(stdout);
   #endif

  int i,init;

   lastr = NULL;
   //initialize objarray; each element points to a different item
   for(i=0;i<MAX_ITEMS;++i)
   {
      objarray[i] = new item(0);  //all init'd to zero
   }

  //initialize optime for the thread to sleep;
  //get a int from random function to sleep 
  int seed = 7919; //prime num   
  srand(seed);  /*initialize random number generator*/ 
  int M = 1000ll;  /* Multiplier */ 
  
  for(i=1; i<MAX_TRANSACTIONS+1; ++i) 
   { 
      double r = ( (double)rand() / (double)(RAND_MAX+1.0) ); //RAND_MAX is defined in stdlib
      double x = (r * M * TEAM_NO); 
      int y = (int) x; 
      optime[i]= abs(y); 
      #ifdef TM_DEBUG
         //   printf("\nValues of i, r, x, and y are: %d, %f, %f, %d\n", i, r, x, y);
         //  fflush(stdout);
      #endif
   }

   //initialize condpool and mutexpool
   for(i=1;i<MAX_TRANSACTIONS+1;++i)
   {
      pthread_mutex_init(&mutexpool[i],NULL);
      pthread_cond_init(&condpool[i],NULL);
      condset[i] = 0; 
      SEQNUM[i] = 0;      
   }

  ZGT_Nsema = MAX_TRANSACTIONS+1 ; //setting the no of semaphore 
  ZGT_Key_sem = TEAM_NO; //setting the key_t data to const 
  
  //semget() gets a array of semaphore for a particular key.Here
  //	creating a semaphore with  key 1
  
   if ((sem= zgt_init_sema(IPC_CREAT))<0)
   {
      cout<< "Error creating semaphores \n";
      exit(1);
   }
  
  ZGT_Semid = sem;
  
  //intialising the semaphore value with 0 to 1 and the rest to 0
  zgt_init_sema_0(ZGT_Semid);
  zgt_init_sema_rest(ZGT_Semid);  
  
   #ifdef TM_DEBUG
      printf("\nleaving TM initialization\n");
      fflush(stdout);
   #endif
};

 
 










