#include "readfile.h"

/* Hash function to choose bucket
 * Input: key used to calculate the hash
 * Output: HashValue;
 */
int hashCode(int key){
   return key % MBUCKETS;
}


/* Functionality insert the data item into the correct position
 *          1. use the hash function to determine which bucket to insert into
 *          2. search for the first empty space within the bucket
 *          	2.1. if it has empty space
 *          	           insert the item
 *          	     else
 *          	          use OpenAddressing to insert the record
 *          3. return the number of records accessed (searched)
 *
 * Input:  fd: filehandler which contains the db
 *         item: the dataitem which should be inserted into the database
 *
 * Output: No. of record searched
 *
 * Hint: You can check the search function to give you some insights
 * Hint2: Don't forget to set the valid bit = 1 in the data item for the rest of functionalities to work
 * Hint3: you will want to check how to use the pwrite function using man pwrite on the terminal
 * 			 ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
 *
 * 	pwrite() writes up to count bytes from the buffer starting  at  buf  to
       the  file  descriptor  fd  at  offset  offset.
 */
int insertItem(int fd,DataItem item){

	struct DataItem data;   //a variable to read in it the records from the db
	struct DataItem data2;   //a variable to read in it the records from the db

	int countSearched = 0;
	item.valid = 1;

	int hashIndex = hashCode(item.key);  				//calculate the Bucket index
	int startingOffset = hashIndex*sizeof(Bucket);		//calculate the starting address of the bucket
	int Offset = startingOffset;						//Offset variable which we will use to iterate on the db

    RESEK:
	ssize_t result = pread(fd,&data,sizeof(DataItem), Offset);
    
    if (data.valid != 1){
    	int result2 = pwrite(fd,&item,sizeof(DataItem), Offset);
	printf("Bucket: %d, Offset: %d, Data: %d, key: %d\n",Offset/BUCKETSIZE,Offset,data.data,data.key);
    }
    else if(data.valid)
    {

    	
    RESEK2:

    	ssize_t result1 = pread(fd,&data2,sizeof(DataItem), 320+Offset);	

	if (data.valid != 1){
	data.offset = 320+Offset;
    	int result2 = pwrite(fd,&item,sizeof(DataItem), Offset+320);
	printf("Bucket: %d, Offset: %d, Data: %d, key: %d\n",Offset/BUCKETSIZE,Offset,data.data,data.key);
    	}
    else if(data.valid)
    {
	Offset +=sizeof(DataItem);  //move the offset to next record
	goto RESEK;
	}
	 //   countSearched++;
	// goto RESEK;

	// if(Offset >= FILESIZE)
	//  { //if reached end of the file start again
	// 		Offset = 0;
	// 		goto RESEK;
	// } else
	//     	  if(Offset >= startingOffset) {
	// 			return -1; //no empty spaces
	//      }
	// 	goto RESEK;
        
	}
   return countSearched;
}


int searchItem(int fd,struct DataItem* item,int *count)
{

	//Definitions
	struct DataItem data;   //a variable to read in it the records from the db
	*count = 0;				//No of accessed records
	int rewind = 0;			//A flag to start searching from the first bucket
	int hashIndex = hashCode(item->key);  				//calculate the Bucket index
	int startingOffset = hashIndex*sizeof(Bucket);		//calculate the starting address of the bucket
	int Offset = startingOffset;						//Offset variable which we will use to iterate on the db

	//Main Loop
	RESEEK:
	//on the linux terminal use man pread to check the function manual
	ssize_t result = pread(fd,&data,sizeof(DataItem), Offset);
	//one record accessed
	(*count)++;
	//check whether it is a valid record or not
    if(result <= 0) //either an error happened in the pread or it hit an unallocated space
	{ 	 // perror("some error occurred in pread");
		  return -1;
    }
    else if (data.valid == 1 && data.key == item->key) {
    	//I found the needed record
    			item->data = data.data ;
    			return Offset;

    } else { //not the record I am looking for
    		Offset +=sizeof(DataItem);  //move the offset to next record
    		if(Offset >= FILESIZE && rewind ==0 )
    		 { //if reached end of the file start again
    				rewind = 1;
    				Offset = 0;
    				goto RESEEK;
    	     } else
    	    	  if(rewind == 1 && Offset >= startingOffset) {
    				return -1; //no empty spaces
    	     }
    		goto RESEEK;
    }
}


/* Functionality: Display all the file contents
 *
 * Input:  fd: filehandler which contains the db
 *
 * Output: no. of non-empty records
 */
int DisplayFile(int fd){

	struct DataItem data;
	int count = 0;
	int Offset = 0;
	for(Offset =0; Offset< FILESIZE;Offset += sizeof(DataItem))
	{
		ssize_t result = pread(fd,&data,sizeof(DataItem), Offset);
		if(result < 0)
		{ 	  perror("some error occurred in pread");
			  return -1;
		} else if (result == 0 || data.valid == 0 ) { //empty space found or end of file
			printf("Bucket: %d, Offset %d:~\n",Offset/BUCKETSIZE,Offset);
		} else {
			pread(fd,&data,sizeof(DataItem), Offset);
			printf("Bucket: %d, Offset: %d, Data: %d, key: %d\n",Offset/BUCKETSIZE,Offset,data.data,data.key);
					 count++;
		}
	}
	return count;
}


/* Functionality: Delete item at certain offset
 *
 * Input:  fd: filehandler which contains the db
 *         Offset: place where it should delete
 *
 * Hint: you could only set the valid key and write just and integer instead of the whole record
 */
int deleteOffset(int fd, int Offset)
{
	struct DataItem dummyItem;
	dummyItem.valid = 0;
	dummyItem.key = -1;
	dummyItem.data = 0;
	int result = pwrite(fd,&dummyItem,sizeof(DataItem), Offset);
	return result;
}




// struct DataItem data;
	
// 	int countSearched = 0;
	
// 	item.valid = 1;
// 	int hashIndex = hashCode(item.key);  				//calculate the Bucket index
// 	int startingOffset = hashIndex*sizeof(Bucket);		//calculate the starting address of the bucket
// 	int Offset = startingOffset;						//Offset variable which we will use to iterate on the db

//     RESEK:
// 	ssize_t result = pread(fd,&data,sizeof(DataItem), Offset);

//     if (data.data.valid == 0){
// 	data.next = NULL;
//     	data.data = item;
//     	int result2 = pwrite(fd,&data,sizeof(DataItemWithPointer), Offset);
// 	printf("Bucket: %d, Offset: %d, Data: %d, key: %d\n",Offset/BUCKETSIZE,Offset,data.data,data.data.key);
//     }
//     else
//     {
//     		countSearched++;
// 	      printf("Before pointer in insertion: ");

// 	    	pointer = data.next;
// 	    	while(pointer){
// 	    		printf("Pointer in insertion: %p",pointer);
// 	    		pointer = pointer->next;
// 		}

// 		// construct last record
// 		dataToInsert.data = item;
// 		// last record points to null
// 		dataToInsert.next = NULL;
// 		// the pointer of the last record now should point to the new record.

// 		// is there a bug in there??
// 	    	pointer->next = &dataToInsert;
//     		// write last record
//     		int result2 = pwrite(fd,&dataToInsert,sizeof(DataItemWithPointer), Offset*2);
// 	}

//    return countSearched;


/* Functionality: using a key, it searches for the data item
 *          1. use the hash function to determine which bucket to search into
 *          2. search for the element starting from this bucket and till it find it.
 *          3. return the number of records accessed (searched)
 *
 * Input:  fd: filehandler which contains the db
 *         item: the dataitem which contains the key you will search for
 *               the dataitem is modified with the data when found
 *         count: No. of record searched
 *
 * Output: the in the file where we found the item
 */

// int searchItem(int fd,struct DataItem* item,int *count)
// {

// 	//Definitions
// 	// for iterative purposes	
// 	struct DataItemWithPointer* pointer;
// 	struct DataItemWithPointer data;
// 	// struct DataItem data;   //a variable to read in it the records from the db
// 	*count = 0;				//No of accessed records
// 	int rewind = 0;			//A flag to start searching from the first bucket
// 	int hashIndex = hashCode(item->key);  				//calculate the Bucket index
// 	int startingOffset = hashIndex*sizeof(Bucket);		//calculate the starting address of the bucket
// 	int Offset = startingOffset;						//Offset variable which we will use to iterate on the db

// 	//Main Loop
// 	RESEEK:
// 	//on the linux terminal use man pread to check the function manual
// 	ssize_t result = pread(fd,&data,sizeof(DataItemWithPointer), Offset);
// 	pointer = data.next;
// 	//one record accessed
// 	(*count)++;
// 	//check whether it is a valid record or not
//     if(result <= 0) //either an error happened in the pread or it hit an unallocated space
// 	{ 	 // perror("some error occurred in pread");
// 		  return -1;
//     	}
//     else if (data.data.valid == 1 && data.data.key == item->key) {
//     	//I found the needed record
//     			item->data = data.data.data ;
//     			return Offset;

//     } else { //not the record I am looking for
//     		while(pointer){
//     			if (data.data.valid == 1 && data.data.key == item->key) {
// 			    	//I found the needed record
// 			    			item->data = data.data.data ;
// 			    			return Offset;
// 			    }
//     			pointer = pointer->next;
//     		}
//     		Offset +=sizeof(DataItem);  //move the offset to next record
//     		if(Offset >= FILESIZE && rewind ==0 )
//     		 { //if reached end of the file start again
//     				rewind = 1;
//     				Offset = 0;
//     				goto RESEEK;
//     	     } else
//     	    	  if(rewind == 1 && Offset >= startingOffset) {
//     				return -1; //no empty spaces
//     	     }
//     		goto RESEEK;
//     }
// }


// /* Functionality: Display all the file contents
//  *
//  * Input:  fd: filehandler which contains the db
//  *
//  * Output: no. of non-empty records
//  */
// int DisplayFile(int fd){

// 	struct DataItemWithPointer data;
// 	int count = 0;
// 	int Offset = 0;
// 	for(Offset =0; Offset< FILESIZE;Offset += sizeof(DataItemWithPointer))
// 	{
// 		ssize_t result = pread(fd,&data,sizeof(DataItemWithPointer), Offset);
// 		if(result < 0)
// 		{ 	  perror("some error occurred in pread");
// 			  return -1;
// 		} else if (result == 0 || data.data.valid == 0 ) { //empty space found or end of file
// 			printf("Bucket: %d, Offset %d:~\n",Offset/BUCKETSIZE,Offset);
// 		} else {
// 			pread(fd,&data,sizeof(DataItemWithPointer), Offset);
// 			printf("Bucket: %d, Offset: %d, Data: %d, key: %d\n",Offset/BUCKETSIZE,Offset,data.data.data,data.data.key);
// 					 count++;
// 		}
// 	}
// 	return count;
// }


// /* Functionality: Delete item at certain offset
//  *
//  * Input:  fd: filehandler which contains the db
//  *         Offset: place where it should delete
//  *
//  * Hint: you could only set the valid key and write just and integer instead of the whole record
//  */
// int deleteOffset(int fd, int Offset)
// {
// 	struct DataItemWithPointer dummyItem;
// 	struct DataItemWithPointer temp;

// 	ssize_t result = pread(fd,&dummyItem,sizeof(DataItemWithPointer), Offset);

// 	struct DataItemWithPointer* pointer = dummyItem.next;

// 	// swap with the next i=until the next is null
// 	while(pointer){
// 			// swap
// 			temp.data = dummyItem.next->data;
// 			dummyItem.next->data = dummyItem.data;
// 			dummyItem.data = temp.data;

// 			pointer = pointer->next;
// 	}
// 	dummyItem.data.valid = 0;
// 	dummyItem.data.key = -1;
// 	dummyItem.data.data = 0;
// 	int result2 = pwrite(fd,&dummyItem,sizeof(DataItemWithPointer), Offset);
// 	return result;
// }

