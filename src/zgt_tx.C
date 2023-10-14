/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/**
 * Implementor: Aman Hogan-Bailey
 * UTAID - 1001830469
 * Instructor: Professor Ashraf Aboulnaga
 * Section: 2238-CSE-5331-002-DBMS MODELS AND IMPLEMENTATION
*/

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>

// start an op with mutex lock and cond wait
extern void *start_operation(long, long);

// finish an op with mutex unlock and con signal
extern void *finish_operation(long);        

// commit/abort based on char value
extern void *do_commit_abort(long, char);   
extern void *process_read_write(long, long, int, char);

// Transaction manager object
extern zgt_tm *ZGT_Sh;	

/**
 * @brief Method that handles "BeginTx tid" in test file. 
 * Inputs a pointer to transaction id, obj pair as a struct.
 * Creates a new transaction node, initializes its data members and
 * adds it to transaction list
 * @param arg
 * @returns None
*/
void *begintx(void *arg)
{
	/**
	 * NOTE: intialise a transaction object. Make sure it is 
	 * done after acquiring the semaphore for the tm and making sure that 
	 * the operation can proceed using the condition variable. when creating
	 * the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
	 * semno as yet as none is waiting on this tx.
	*/

	struct param *node = (struct param*)arg; // get tid and count
	start_operation(node->tid, node->count); 
	zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node

	zgt_p(0); // Lock Tx manager; Add node to transaction list
		tx->nextr = ZGT_Sh->lastr;
		ZGT_Sh->lastr = tx;   
	zgt_v(0); // Release tx manager 
	
	fprintf(ZGT_Sh->logfile, "T%ld\t%c \tBeginTx\n", node->tid, node->Txtype);
	fflush(ZGT_Sh->logfile);
	finish_operation(node->tid);
	pthread_exit(NULL);
}

/**
 * @brief Reads transaction; Inputs a pointer to structure that contans
 * tx id and object no to read. Reads the object if 
 * the object is not yet present in hash table or same tx holds a lock on it. 
 * Otherwise waits until the lock is released 
 * @param arg
 * @returns None
*/
void *readtx(void *arg)
{
	// Structure that holds tid, obno, count, and Txtyp
	struct param *node = (struct param*) arg;

	// Begin Operation
	start_operation(node->tid, node->count);

	// Pointer to current transaction
	zgt_tx* txptr = get_tx(node->tid); 
	txptr->set_lock(node->tid, 1, node->obno, node->count, 'R');

	finish_operation(node->tid);

	pthread_exit(NULL);
}

/**
 * @brief writes transaction
 * @param arg
 * @returns None
*/
void *writetx(void *arg)
{ 
	//do the operations for writing; similar to readTx
  	struct param *node = (struct param*)arg;	// struct parameter that contains
  	//do the operations for writing; similar to readTx. Write your code
  	return(0);
}

/**
 * @brief common method to process read/write: just a suggestion
*/
void *process_read_write(long tid, long obno,  int count, char mode)
{
	return(0);
}

/**
 * @brief aborts transaction
 * @param arg
 * @returns None
*/
void *aborttx(void *arg)
{
  struct param *node = (struct param*)arg; 
  //TODO: write your code
  pthread_exit(NULL);			
}

/**
 * @brief commits transaction
 * @param arg
 * @returns None
*/
void *committx(void *arg)
{
	//remove the locks/objects before committing
	// get tid and count
	struct param *node = (struct param*)arg;

	//TODO: write your code
	pthread_exit(NULL);
}

/**
 * @brief called from commit/abort with appropriate parameter to do the actual operation
 * Make sure you give error messages if you are trying to commit/abort a non-existant tx
 * @param t
 * @param status
*/
void *do_commit_abort(long t, char status)
{
	// TODO: write your code
	return(0);
}

/**
 * @brief Locks mutex pool and makes threads of same transaction wait until process is finished
 * @param tid Transaction ID
 * @param count Current operation number (compared with the condset[tid] value to decide whether to wait)
*/
void *start_operation(long tid, long count)
{

	// Lock mutex[t] to make other
	pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);
	
	// threads of same transaction to wait
	while(ZGT_Sh->condset[tid] != count)
	{
		// wait if condset[t] is != count
		pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
	}

	return(0);
}

/**
 * @brief Signals the conditional broadcast
 * @param tid Transaction ID
 * @returns None
*/
void *finish_operation(long tid)
{
	ZGT_Sh->condset[tid]--;	// decreases condset[tid] for allowing the next op
	pthread_cond_broadcast(&ZGT_Sh->condpool[tid]); // other waiting threads of same tx
	pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
	return(0);
}

/**
 * @brief Makes a linear scan over the linked list of transaction nodes 
 * and returns the reference of the required node if found
 * @param tid1 Transaction
 * @param zgt_tx* pointer to a transaction, NULL otherwise
*/
zgt_tx* get_tx(long tid1)
{  
	// temp pointer transaction, and pointer to last transaction
	zgt_tx *txptr, *lastr1;

	// While transaction is not NULL
	if(ZGT_Sh->lastr != NULL)
	{	
		lastr1 = ZGT_Sh->lastr;
		for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
			if (txptr->tid == tid1)							
				return txptr; 
		return (NULL);			
	}
	return(NULL);
}

/**
 * @brief Transaction class constructor. Initializes transaction id, status, and thread id.
 * @param tid Transaction id (Ex: Tx1, Tx2, etc)
 * @param Txstatus Status: P for Processing, W for Wiat, A for Abort, E for commit in progress
 * @param type Type of Transaction, e.g Read or Write
 * @param thrid Unique thread id
 * @returns None
*/
zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid)
{
	this->lockmode = (char)' '; 
	this->Txtype = type; 
	this->sgno = 1;
	this->tid = tid;
	this->obno = -1; // Always 1
	this->status = Txstatus; // Status: P for Processing, W for Wiat, A for Abort, E for commit in progress
	this->pid = thrid;
	this->head = NULL;
	this->nextr = NULL;
	this->semno = -1;
}

/**
 * @brief Removes the transaction from the transaction manager
 * @param None
 * @returns None
*/
int zgt_tx::remove_tx ()
{
	// Temporary pointer
	zgt_tx *txptr;

	// Pointer to the last transaction in the transaction list
	zgt_tx *lastr1;
	lastr1 = ZGT_Sh->lastr;

	// Sacn through list of transactions, starting from the last transaction
	for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr)
	{	
		// If the TID mathces the Tx to be removed, remove it from the list
		if (txptr->tid == this->tid)
		{	
			lastr1->nextr = txptr->nextr;	
			return(0);
		}

		// Update temporary pointer
		else
		{
			lastr1 = txptr->nextr;
		} 			
	}

	fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%ld that does not exist\n", this->tid);
	fflush(ZGT_Sh->logfile);
	printf("Trying to Remove a Tx:%ld that does not exist\n", this->tid);
	fflush(stdout);
	return(-1);
}

/**
 * @brief sets lock on objno1 with lockmode1 for a tx
 * @param tid1 Transaction ID
 * @param sgno1 The number: 1
 * @param obno1 Object number
 * @param count Current operation number (compared with the condset[tid] value to decide whether to wait)
 * @param lockmode1 R for shared lock, W for exclusive lock
 * @return2 0 for succes, -1 for failure
*/
int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1)
{
	//if the thread has to wait, block the thread on a semaphore from the
	//sempool in the transaction manager. Set the appropriate parameters in the
	//transaction list if waiting.
	//if successful  return(0); else -1
	//write your code
	
	// Current transaction
	zgt_tx *txptr = get_tx(tid1);

	zgt_p(0); // set lock
		zgt_hlink *hash_txptr = ZGT_Ht->find(sgno1, obno1); // Pointer to transaction on hashtable
	zgt_p(0); // free lock

	// If there was not a transaction in the hashtable, add it to the hashtable
	if (hash_txptr == NULL)
	{
		zgt_p(0); // set lock
			ZGT_Ht->add(txptr,sgno1, obno1, lockmode1);
		zgt_v(0); // free lock

		// Perform operation
		txptr->perform_readWrite(tid1, obno1, lockmode1);
	}

	// Else if the transaction was found inside the hash table
	else if (hash_txptr->tid == txptr->tid)
	{
		// Perform operation
		txptr->perform_readWrite(tid1, obno1, lockmode1);
	}

	// NOTE: This condition kept occuring for me when trancations were wating on other transactions
	// Else if the transaction cannot proceed
	else
	{
		// Check if an object is being held by a transaction
		zgt_p(0); // set lock
			zgt_hlink *zgt_obj_held = ZGT_Ht->findt(tid1, sgno1, obno1); // Object held by transaction
		zgt_v(0); // free lock

		// If an object is being held by a transaction, perform operation
		if(zgt_obj_held != NULL)
		{
			txptr->perform_readWrite(tid1, obno1, lockmode1);
		}

		// If no object is being held by a transaction
		else
		{
			// Get number of transactions waiting on semphaore to realease the other transactions waiting on it
			int zgt_wait = zgt_nwait(hash_txptr->tid);

			// Exclusive Lock
			if((lockmode1 == 'R' && hash_txptr->lockmode == 'W' ) || (lockmode1 == 'R' && hash_txptr->lockmode == 'R' && zgt_wait > 0) || (lockmode1 == 'W'))
			{
				txptr->status = TR_WAIT;
				txptr->obno = obno1;
				txptr->lockmode = lockmode1;
				
				// Set the number of semaphores
				txptr->setTx_semno(hash_txptr->tid,hash_txptr->tid);
				zgt_p(hash_txptr->tid); // Set lock
					txptr->lockmode = ' ';
					txptr->obno = -1;
					txptr->status = TR_ACTIVE;
					txptr->perform_readWrite(tid1, obno1, lockmode1);
				zgt_v(hash_txptr->tid); // Free lock
			}

			// Shared Lock
			else
			{
				// Perform operation
				txptr->perform_readWrite(tid1, obno1, lockmode1);
			}
		}
	}
	
	return(0);
}

/**
 * @brief frees all locks owned by the transaction and remove the objects from 
 * the hash table and release all Tx's waiting on this Tx
 * @param None
 * @returns 0 on success, failure otherwise
*/
int zgt_tx::free_locks()
{
	// head of transaction
	zgt_hlink* temp = head;  
  
	// scan the Tx obj list
	for(temp; temp != NULL; temp = temp->nextp)
	{	
		fprintf(ZGT_Sh->logfile, "%ld : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
		fflush(ZGT_Sh->logfile);

		if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1)
		{
			printf(":::ERROR:node with tid:%ld and onjno:%ld was not found for deleting", this->tid, temp->obno);		// Release from hash table
			fflush(stdout);
		}

		else 
		{
			#ifdef TX_DEBUG
				printf("\n:::Hash node with Tid:%ld, obno:%ld lockmode:%c removed\n",
				temp->tid, temp->obno, temp->lockmode);
				fflush(stdout);
			#endif
		}
	}

	fprintf(ZGT_Sh->logfile, "\n");
	fflush(ZGT_Sh->logfile);
	return(0);
}		

/**
 * @brief remove the transaction and free all associate dobjects.
 * can be used for commit of the transaction.
 * @param None
 * @returns 0 on success, otherwise on failure
*/
int zgt_tx::end_tx() 
{
	zgt_tx *linktx, *prevp;
	linktx = prevp = ZGT_Sh->lastr;
	while (linktx)
	{
		if (linktx->tid  == this->tid) 
			break;
		prevp  = linktx;
		linktx = linktx->nextr;
	}

	if (linktx == NULL) 
	{
		printf("\ncannot remove a Tx node; error\n");
		fflush(stdout);
		return (1);
	}

	if (linktx == ZGT_Sh->lastr) 
		ZGT_Sh->lastr = linktx->nextr;
	
	else 
	{
		prevp = ZGT_Sh->lastr;
		while (prevp->nextr != linktx) 
			prevp = prevp->nextr;
		prevp->nextr = linktx->nextr;    
	}

	return(0);
}

/**
 * @brief - Deprecated
 * 
*/
int zgt_tx::cleanup()
{
  return(0);
}

/**
 * @brief prints the tx list (TX_DEBUG should be defined in the Makefile to print)
 * @param None
 * @returns None
*/
void zgt_tx::print_tm()
{
	zgt_tx *txptr;
	#ifdef TX_DEBUG
		printf("printing the tx  list \n");
		printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
		fflush(stdout);
	#endif

	txptr=ZGT_Sh->lastr;
	while (txptr != NULL) 
	{
		#ifdef TX_DEBUG
			printf("%ld\t%c\t%ld\t%ld\t%c\t%c\t%d\n", 
				txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
			fflush(stdout);
		#endif
		txptr = txptr->nextr;
	}
	fflush(stdout);
}

/**
 * @brief prints debug info (WAIT)
 * @param None
 * @returns None
*/
void zgt_tx::print_wait()
{
	printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
	printf("\n");
	return;
}

/**
 * @brief prints debug info (LOCK)
 * @param None
 * @returns None
*/
void zgt_tx::print_lock()
{
	printf("\n    SGNO        OBNO        TID        PID   L\n");
	printf("\n");
	return;
}

/**
 * @brief performs the acutual read/write operation based  on the lockmode
 * @param tid Transaction id (Ex: Tx1, Tx2, etc)
 * @param obno1 Object number
 * @param lockmode1 R for shared lock, W for exclusive lock
 * @returns None
 * 
*/
void zgt_tx::perform_readWrite(long tid, long obno, char lockmode){

	// TODO: Write your own code
	int values = ZGT_Sh->objarray[obno]->value;

	// TxId Txtype Operation ObId:Obvalue:optime LockType Status TxStatus

	// WriteLock
	if(lockmode == 'W')   
	{
		ZGT_Sh->objarray[obno]->value = values + 1;  
		fprintf(ZGT_Sh->logfile, "Transaction : %ld\t\t\ttWriteTx\t\t\t%ld:%d:%d\t\t\tWriteLock\t\t\tGranted\t\t\t %c\n", this->tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
		fflush(ZGT_Sh->logfile);
	}

	// ReadLock
	else  
	{
		ZGT_Sh->objarray[obno]->value=values - 1;
		fprintf(ZGT_Sh->logfile, "Transaction : %ld\t\t\t ReadTx\t\t\t %ld:%d:%d \t\t\t ReadLock\t\t\t Granted\t\t\t %c\n", this->tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
		fflush(ZGT_Sh->logfile);
	}
}

/**
 * @brief Sets the semno in the Tx when another tx waits on it.
 * The same number is the same as the tx number on which a Tx is waiting
 * @param tid Transaction id (Ex: Tx1, Tx2, etc)
 * @param semno number that indicates that some other Tx is waiting on this tx to release object
 * @returns 0 on success, otherwize, failure
*/
int zgt_tx::setTx_semno(long tid, int semno)
{
	zgt_tx *txptr;
	txptr = get_tx(tid);

	if (txptr == NULL)
	{
		printf("\n:::ERROR:Txid %ld wants to wait on sem:%d of tid:%ld which does not exist\n", this->tid, semno, tid);
		fflush(stdout);
		exit(1);
	}

	// just to be safe
	if ((txptr->semno == -1)|| (txptr->semno == semno))
	{  
		txptr->semno = semno;
		return(0);
	}

	else if (txptr->semno != semno)
	{
		#ifdef TX_DEBUG
			printf(":::ERROR Trying to wait on sem:%d, but on Tx:%ld\n", semno, txptr->tid);
			fflush(stdout);
		#endif
		exit(1);
	}

	return(0);
}