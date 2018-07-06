#include "AM.h"
#include "bf.h"
#include "my_structs.h"
#include "defn.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define AM_ID '#'
#define AM_INDEX_ID '%'
#define AM_DATA_ID '^'


BF_ErrorCode BF_AllocateIndexBlock(int fd,BF_Block *block,void *value,int flag,int parent){
	if(BF_AllocateBlock(fd,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
		return BF_ERROR;
	}
	char* data = BF_Block_GetData(block);
        char c = AM_INDEX_ID;
	int s1=AM_File[fd]->size1;
	int counter = flag ? 1 : 0;
	memcpy(data,&c,sizeof(char));
	memcpy(data + sizeof(char), &parent, sizeof(int));
	memcpy(data + sizeof(char) + sizeof(int), &counter, sizeof(int));
	//we begin copying there because we want to store a pointer for the parent block and counter
	if(flag)
		memcpy(data + sizeof(char) + 3*sizeof(int),value, s1);
	return BF_OK;
}


BF_ErrorCode BF_AllocateDataBlock(int fd,BF_Block *block,void *value1,void *value2,int flag){
	if(BF_AllocateBlock(fd,block) !=BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
		return BF_ERROR;
	}
	char* data = BF_Block_GetData(block);
        char c = AM_DATA_ID;
	int next = -1;
	int s1=AM_File[fd]->size1,s2=AM_File[fd]->size2;
        int counter = flag ? 1 : 0;
        memcpy(data,&c,sizeof(char));
	memcpy(data + sizeof(char), &next, sizeof(int));
	memcpy(data + sizeof(char) + sizeof(int), &counter, sizeof(int));
        if(flag){
		memcpy(data + sizeof(char) + 2*sizeof(int),value1,s1);
		memcpy(data + sizeof(char) + 2*sizeof(int) + s1,value2, s2);
        }
	return BF_OK;
}

int setNext(int fd, int blockNum, int next){
        BF_Block *block;
        BF_Block_Init(&block);
        char* data;
	char id;

        if(BF_GetBlock(AM_File[fd]->fileDesc,blockNum,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
	memcpy(&id, data, sizeof(char));
	if(id != AM_DATA_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
		return AME_ERROR;//wrong blockNum
	}
	memcpy(data + sizeof(char), &next, sizeof(int));
        BF_Block_SetDirty(block);
        if(BF_UnpinBlock(block) != BF_OK)
                BF_PrintError(BF_ERROR);
	BF_Block_Destroy(&block);
	return AME_OK;
}

int getNext(int fd,int blockNum){
        BF_Block *block;
        BF_Block_Init(&block);
        char* data;
	int next;
	char id;

        if(BF_GetBlock(AM_File[fd]->fileDesc,blockNum,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
	memcpy(&id, data, sizeof(char));
	if(id != AM_DATA_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
		return AME_ERROR;//wrong blockNum
	}
	memcpy(&next, data + sizeof(char), sizeof(int));
        if(BF_UnpinBlock(block) != BF_OK)
                BF_PrintError(BF_ERROR);
	BF_Block_Destroy(&block);
	return next;
}

int getParent(int fd,int blockNum){
        BF_Block *block;
        BF_Block_Init(&block);
        char* data;
        int parent;
	char id;

        if(BF_GetBlock(AM_File[fd]->fileDesc,blockNum,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
	memcpy(&id, data, sizeof(char));
        if(id != AM_INDEX_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
                return AME_ERROR;//wrong blockNum
	}
        memcpy(&parent, data + sizeof(char), sizeof(int));
        if(BF_UnpinBlock(block) != BF_OK)
                BF_PrintError(BF_ERROR);
	BF_Block_Destroy(&block);
        return parent;
}


int isGreaterString(char *a,char *b){
	if(strcmp(a,b)>0)
		return 0;
	return 1;
}

int isGreaterInteger(int a,int b){
        if( a >= b )
                return 0;
        return 1;
}

int isGreaterFloat(float a,float b){
        if( a >= b )
                return 0;
        return 1;
}


int isGreater(char *a,char *b,int fd){
	if(AM_File[fd]->type1 == STRING){
		return isGreaterString(a,b);
	}
	else{
		if((float*)a < (float*)b){
			return 0;
		}
	}//here wee check if our value is greater than the key depending on the value type
	return 1;
}

int findBlockToInsert(int fd,void* value,int *parent,int s1,int root,char type1){
	BF_Block *block;
  	BF_Block_Init(&block);
        char key1[s1];
        int key2 , v1;
        float key3, v2;

	int flag = 0;
	int blockNum = root;
	char* data;
	//To insert a record we have to find the rigth DATABlock starting from the given blockNum
    	if(BF_GetBlock(fd,root,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
		return AME_ERROR;
	}
	data = BF_Block_GetData(block);
	char id;
	memcpy(&id,data,sizeof(char));
	if(id != AM_INDEX_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
		return AME_ERROR;//wrong blockNum
	}
	int counter,total_keys;
	while(id != AM_DATA_ID){
		memcpy(&total_keys,data + sizeof(char) + sizeof(int),sizeof(int));
		counter = 0;
		while(!flag){//the flag being 1 shows us that we havent found the right blockpointer
			//we get the first key of the block
	                if(type1 == STRING){
	                        memcpy(key1,data + sizeof(char) + 3*sizeof(int)+ counter*(s1+sizeof(int) ),s1);
        	                flag = isGreaterString(value,key1);//check if our value is greater than the key
				if(!strcmp(value,key1)) flag = 0;
	                }
        	        else if(type1 == INTEGER){
                	        memcpy(&key2,data + sizeof(char) + 3*sizeof(int)+ counter*(s1+sizeof(int)),s1);
                        	memcpy(&v1,value,sizeof(int));
                        	flag = isGreaterInteger(v1,key2);//check if our value is greater than the key
                	}
                	else{
                        	memcpy(&key3,data + sizeof(char) + 3*sizeof(int)+ counter*(s1+sizeof(int)),s1);
                        	memcpy(&v2,value,sizeof(float));
                        	flag = isGreaterFloat(v2,key3);//check if our value is greater than the key
                	}
			//here wee check if our value is greater than the key depending on the value type
			if(flag){//if flag == 0 then its not so we found our pointer which is the left Blockpointer of the current key
				*parent = blockNum;
				memcpy(&blockNum,data + sizeof(char) + 2*sizeof(int) + counter*(sizeof(int)+s1),sizeof(int));
				break ;
			}
			counter++;
			if(counter == total_keys){
				*parent = blockNum;
				memcpy(&blockNum,data + sizeof(char) + 2*sizeof(int) + counter*(sizeof(int)+s1),sizeof(int));
				break ;
			}
		}
		if(BF_UnpinBlock(block) != BF_OK)
			BF_PrintError(BF_ERROR);
		flag = 0;
		if(BF_GetBlock(fd,blockNum,block) != BF_OK){//now we are gonna start checking in the next block
			AM_errno = AME_FULL_MEMORY_ERROR;
			return AME_ERROR;
		}
		data = BF_Block_GetData(block);
		memcpy(&id,data,sizeof(char));
	}
	if(BF_UnpinBlock(block) != BF_OK)
		BF_PrintError(BF_ERROR);
	BF_Block_Destroy(&block);
	return blockNum;
}

int isFullDataBlock(int fd,int blockNum){
        BF_Block *block;
        BF_Block_Init(&block);
	int counter;
	char id;
	char* data;

        if(BF_GetBlock(AM_File[fd]->fileDesc,blockNum,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
	data = BF_Block_GetData(block);
        memcpy(&id, data, sizeof(char));
        if(id != AM_DATA_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
                return AME_ERROR;//wrong blockNum
	}
        memcpy(&counter,data + sizeof(char) + sizeof(int),sizeof(int));
	if(BF_UnpinBlock(block) != BF_OK)
		BF_PrintError(BF_ERROR);
	BF_Block_Destroy(&block);
	if(counter>=AM_File[fd]->max_data_records)
		return 1;
	return 0;
}

int isFullIndexBlock(int fd,int blockNum){
        BF_Block *block;
        BF_Block_Init(&block);
        int counter;
	char id;
	char* data;

        if(BF_GetBlock(AM_File[fd]->fileDesc,blockNum,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
        memcpy(&id, data, sizeof(char));
        if(id != AM_INDEX_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
                return AME_ERROR;//wrong blockNum
	}
        memcpy(&counter,data + sizeof(char) + sizeof(int),sizeof(int));
	if(BF_UnpinBlock(block) != BF_OK)
		BF_PrintError(BF_ERROR);
	BF_Block_Destroy(&block);
        if(counter>=AM_File[fd]->max_index_records)
                return 1;
        return 0;
}

//Sorted insert
int insertInDataBlock(int fd,int blockToInsert,void* value1,void* value2){
        BF_Block *block;
        BF_Block_Init(&block);
        int counter,counter2;
	char id;
	int s1=AM_File[fd]->size1, s2=AM_File[fd]->size2;
	char* data;
        char key1[AM_File[fd]->size1];
        int key2;
        float key3;
	int v1;
	float v2;

        if(BF_GetBlock(AM_File[fd]->fileDesc,blockToInsert,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
        memcpy(&id, data, sizeof(char));
        if(id != AM_DATA_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
                return AME_ERROR;//wrong blockNum
	}
        memcpy(&counter,data + sizeof(char) + sizeof(int),sizeof(int));
	counter2 = 0;
	while(counter2 != counter){
                if(AM_File[fd]->type1 == STRING){
                        memcpy(key1,data + sizeof(char) + 2*sizeof(int)+ counter2*(s1+s2),s1);
			if(isGreaterString(value1,key1)) break;
                }
                else if(AM_File[fd]->type1 == INTEGER){
                        memcpy(&key2,data + sizeof(char) + 2*sizeof(int)+ counter2*(s1+s2),sizeof(int));
			memcpy(&v1,value1,sizeof(int));
                        if(isGreaterInteger(v1,key2)) break;
                }
                else{
                        memcpy(&key3,data + sizeof(char) + 2*sizeof(int)+ counter2*(s1+s2),sizeof(int));
			memcpy(&v2,value1,sizeof(float));
                        if(isGreaterFloat(v2,key3)) break;
                }
		counter2++;
	}
	char* temp = malloc(((counter-counter2)*(s1+s2))*sizeof(char));
	memcpy(temp,data + sizeof(char) + 2*sizeof(int) + counter2*(s1+s2),((counter-counter2)*(s1+s2))*sizeof(char));
	memcpy(data + sizeof(char) + 2*sizeof(int) + counter2*(s1+s2),value1,s1);
	memcpy(data + sizeof(char) + 2*sizeof(int) + counter2*(s1+s2) + s1,value2,s2);
	memcpy(data + sizeof(char) + 2*sizeof(int) + (counter2+1)*(s1+s2),temp,((counter-counter2)*(s1+s2))*sizeof(char));
	counter++;
	memcpy(data + sizeof(char) + sizeof(int),&counter,sizeof(int));
	BF_Block_SetDirty(block);
	if(BF_UnpinBlock(block) != BF_OK)
		BF_PrintError(BF_ERROR);
	free(temp);
	BF_Block_Destroy(&block);

	return 1;
}

int dataBlockSplit(int fd, int blockNum, void* value,void* value1,void* value2){
        BF_Block *block;
        BF_Block_Init(&block);
        int s1=AM_File[fd]->size1, s2=AM_File[fd]->size2;
        char* data;
	int recToComp;
	char key1[s1],v0[s1];
	int key2,v1;
	float key3,v2;
	int flag = 0;
	int cr=0,cl=0;
	int next;
	int temp_counter;
	char* temp;
	char* temp2;
	char id;
	int newBlockNum;
        if(BF_GetBlock(AM_File[fd]->fileDesc,blockNum,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
	memcpy(&id,data,sizeof(char));
	if(id != AM_DATA_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
		return AME_ERROR;//wrong blockNum
	}
	if(AM_File[fd]->max_data_records % 2){//block has odd num of records
		recToComp = AM_File[fd]->max_data_records/2;//starts from 0 to max-1
                if(AM_File[fd]->type1 == STRING){
                        memcpy(key1,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),s1);
			if(isGreaterString(value1,key1)) flag = 1;
			if(!strcmp(key1,value1)) flag = 2;
			while(cr != recToComp){
				memcpy(v0,data + sizeof(char) + 2*sizeof(int)+ (recToComp+cr+1)*(s1+s2),s1);
				if(strcmp(key1,v0)) break;
				cr++;
			}
			while(cl != recToComp){
				memcpy(v0,data + sizeof(char) + 2*sizeof(int)+ (recToComp-cl-1)*(s1+s2),s1);
				if(strcmp(key1,v0)) break;
				cl++;
			}
                }
                else if(AM_File[fd]->type1 == INTEGER){
                        memcpy((char*)&key2,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&v1,value1,sizeof(int));
                        if(isGreaterInteger(v1,key2)) flag = 1;
			if(key2 == v1) flag = 2;
                        while(cr != recToComp){
                                memcpy(&v1,data + sizeof(char) + 2*sizeof(int)+ (recToComp+cr+1)*(s1+s2),s1);
                                if(v1 != key2) break;
                                cr++;
                        }
                        while(cl != recToComp){
                                memcpy(&v1,data + sizeof(char) + 2*sizeof(int)+ (recToComp-cl-1)*(s1+s2),s1);
                                if(v1 != key2) break;
                                cl++;
                       }
                }
                else{
                        memcpy((char*)&key3,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&v2,value1,sizeof(float));
                        if(isGreaterFloat(v2,key3)) flag = 1;
			if(key3 == v2) flag = 2;
                        while(cr != recToComp){
                                memcpy(&v2,data + sizeof(char) + 2*sizeof(int)+ (recToComp+cr+1)*(s1+s2),s1);
                                if(v2 != key3) break;
                                cr++;
                        }
                        while(cl != recToComp){
                                memcpy(&v2,data + sizeof(char) + 2*sizeof(int)+ (recToComp-cl-1)*(s1+s2),s1);
                                if(v2 != key3) break;
                                cl++;
                        }
                }
		if(cr>cl){
			if(flag==0 || flag==2){
				temp2 = malloc((cl+1)*(s1+s2));
				memcpy(temp2,data + sizeof(char) + 2*sizeof(int)+ (recToComp-cl)*(s1+s2),(cl+1)*(s1+s2));
			}
			else if(flag==1){
				temp2 = malloc(cl*(s1+s2));
                                memcpy(temp2,data + sizeof(char) + 2*sizeof(int)+ (recToComp-cl)*(s1+s2),cl*(s1+s2));
			}
		}
		if(cr<cl){
                        if(flag==0 || flag==2){
                                temp2 = malloc(cr*(s1+s2));
                                memcpy(temp2,data + sizeof(char) + 2*sizeof(int)+ (recToComp+1)*(s1+s2),cr*(s1+s2));
                        }
                        else if(flag==1){
                                temp2 = malloc((cr+1)*(s1+s2));
                                memcpy(temp2,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),(cr+1)*(s1+s2));
                        }
		}
		else{//cr==cl
                        if(flag==0 || flag==2){
                                temp2 = malloc(cr*(s1+s2));
                                memcpy(temp2,data + sizeof(char) + 2*sizeof(int)+ (recToComp+1)*(s1+s2),cr*(s1+s2));
                        }
                        else if(flag==1){
                                temp2 = malloc(cr*(s1+s2));
                                memcpy(temp2,data + sizeof(char) + 2*sizeof(int)+ (recToComp+1)*(s1+s2),cr*(s1+s2));
                        }
		}

		if(flag==0 || flag==2){//flag=0 [0,recToComp] stays here,[recToComp+1,max-1] in new block
			temp = malloc(recToComp*(s1+s2));
			memcpy(temp,data + sizeof(char) + 2*sizeof(int)+ (recToComp+1)*(s1+s2),recToComp*(s1+s2));
			memcpy(&next,data + sizeof(char) ,sizeof(int));
			recToComp++;
			//memcpy(data + sizeof(char) + 2*sizeof(int),&recToComp,sizeof(int));
			if(cl>cr){
				temp_counter = recToComp + cr;//copy same keys in left block
				memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
				//doesn't need to write again block's records they already are here,just increase counter
				//memcpy(data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),temp2,cr*(s1+s2));
			}
			else if(cl<cr){
				temp_counter = recToComp - cl-1;
				memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
			}
			else{//cl==cr
				temp_counter = recToComp + cr;//or cl
				memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
				//same here just increase counter
				//memcpy(data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),temp2,cr*(s1+s2));
			}
			BF_Block_SetDirty(block);

		        if(BF_UnpinBlock(block) != BF_OK)
		                BF_PrintError(BF_ERROR);
			if(cl>=cr && flag==2){
				if(insertInDataBlock(fd,blockNum,value1,value2) == AME_ERROR)
					return AME_ERROR;
			}
	    		if(BF_AllocateDataBlock(fd,block,value1,value2,0) != BF_OK)
				return AME_ERROR;
			if(BF_GetBlockCounter(AM_File[fd]->fileDesc,&newBlockNum) != BF_OK){
				AM_errno = AME_FULL_MEMORY_ERROR;
				return AME_ERROR;
			}
			if(setNext(fd,blockNum,newBlockNum-1) == AME_ERROR)
				return AME_ERROR;

			data = BF_Block_GetData(block);
			recToComp--;
			memcpy(data + sizeof(char),&next,sizeof(int));
                        if(cl>cr){
                                temp_counter = recToComp - cr;//copy same keys in left block
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
				memcpy(data + sizeof(char) + 2*sizeof(int),temp + cr*(s1+s2),temp_counter*(s1+s2));
                        }
                        else if(cl<cr){
                                temp_counter = recToComp + cl+1;
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
				memcpy(data + sizeof(char) + 2*sizeof(int),temp2,(cl+1)*(s1+s2));
				memcpy(data + sizeof(char) + 2*sizeof(int) + (cl+1)*(s1+s2),temp,recToComp*(s1+s2));
				if(flag==2){
					if(insertInDataBlock(fd,newBlockNum-1,value1,value2) == AME_ERROR)
						return AME_ERROR;
				}
                        }
                        else{//cl==cr
                                temp_counter = recToComp - cr;//or cl
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
				memcpy(data + sizeof(char) + 2*sizeof(int),temp + cr*(s1+s2),temp_counter*(s1+s2));
                        }
                        BF_Block_SetDirty(block);
                        if(BF_UnpinBlock(block) != BF_OK)
                                BF_PrintError(BF_ERROR);
			if(flag==0){
				if(insertInDataBlock(fd,newBlockNum-1,value1,value2) == AME_ERROR)
					return AME_ERROR;
			}
		}
		else if(flag==1){
			temp = malloc((recToComp+1)*(s1+s2));//we split the block with the mean block going right
			memcpy(temp,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),(recToComp + 1)*(s1+s2));
                        memcpy(&next,data + sizeof(char),sizeof(int));
                        if(cl>cr){//we move back the mean+ the cr records to the left block
                                temp_counter = recToComp + cr+1;
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
                                //memcpy(data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),temp2,(cr+1)*(s1+s2));
                        }
                        else if(cl<cr){//we will move the remaining cl records to right block
                                temp_counter = recToComp - cl;
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
                        }
                        else{//cl==cr
                                temp_counter = recToComp - cl;//the cl records will move to right block
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
                                //memcpy(data + sizeof(char) + 3*sizeof(int),temp2,(cr+1)*(s1+s2));
                        }

                        BF_Block_SetDirty(block);
                        if(BF_UnpinBlock(block) != BF_OK)
                                BF_PrintError(BF_ERROR);
                        if(BF_AllocateDataBlock(fd,block,value1,value2,0) != BF_OK)
                                return AME_ERROR;
                        if(BF_GetBlockCounter(AM_File[fd]->fileDesc,&newBlockNum) != BF_OK){
				AM_errno = AME_INVALID_FILE_ERROR;
                                return AME_ERROR;
			}
                        if(setNext(fd,blockNum,newBlockNum-1) == AME_ERROR)
				return AME_ERROR;
                        data = BF_Block_GetData(block);
                        recToComp++;
                        memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));
                        memcpy(data + sizeof(char),&next,sizeof(int));
                        memcpy(data + sizeof(char) + 2*sizeof(int),temp,recToComp*(s1+s2));
                        if(cl>cr){
                                temp_counter = recToComp - cr-1;//copy same keys in left block
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
                                memcpy(data + sizeof(char) + 2*sizeof(int),temp + (cr+1)*(s1+s2),temp_counter*(s1+s2));
                        }
                        else if(cl<cr){
                                temp_counter = recToComp + cl;
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
                                memcpy(data + sizeof(char) + 2*sizeof(int),temp2,cl*(s1+s2));
                                memcpy(data + sizeof(char) + 2*sizeof(int) + cl*(s1+s2),temp,recToComp*(s1+s2));
                        }
                        else{//cl==cr
                                temp_counter = recToComp + cl;
                                memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
				memcpy(data + sizeof(char) + 2*sizeof(int),temp2,cl*(s1+s2));
                                memcpy(data + sizeof(char) + 2*sizeof(int) + cl*(s1+s2),temp,recToComp*(s1+s2));
                        }
                        BF_Block_SetDirty(block);
                        if(BF_UnpinBlock(block) != BF_OK)
                                BF_PrintError(BF_ERROR);
                        if(insertInDataBlock(fd,blockNum,value1,value2) == AME_ERROR)//we are sure here that value1 != key[recToComp]
				return AME_ERROR;
                }
	}
	else{//even
		recToComp = AM_File[fd]->max_data_records/2;//starts from 0 to max-1
                if(AM_File[fd]->type1 == STRING){
                        memcpy(key1,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),s1);
                        if(isGreaterString(value1,key1)) flag = 1;
                        if(!strcmp(key1,value1)) flag = 2;
                        while(cr != recToComp - 1){//max/2-1
                                memcpy(v0,data + sizeof(char) + 2*sizeof(int)+ (recToComp+cr+1)*(s1+s2),s1);
                                if(strcmp(key1,v0)) break;
                                cr++;
                        }
                        while(cl != recToComp){//max/2
                                memcpy(v0,data + sizeof(char) + 2*sizeof(int)+ (recToComp-cl-1)*(s1+s2),s1);
                                if(strcmp(key1,v0)) break;
                                cl++;
                        }
                }
                else if(AM_File[fd]->type1 == INTEGER){
                        memcpy((char*)&key2,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&v1,value1,sizeof(int));
                        if(isGreaterInteger(v1,key2)) flag = 1;
                        if(key2 == v1) flag = 2;
                        while(cr != recToComp - 1){
                                memcpy(&v1,data + sizeof(char) + 2*sizeof(int)+ (recToComp+cr+1)*(s1+s2),s1);
                                if(v1 != key2) break;
                                cr++;
                        }
                        while(cl != recToComp){
                                memcpy(&v1,data + sizeof(char) + 2*sizeof(int)+ (recToComp-cl-1)*(s1+s2),s1);
                                if(v1 != key2) break;
                                cl++;
                       }
                }
                else{
                        memcpy((char*)&key3,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&v2,value1,sizeof(float));
                        if(isGreaterFloat(v2,key3)) flag = 1;
                        if(key3 == v2) flag = 2;
                        while(cr != recToComp - 1){
                                memcpy(&v2,data + sizeof(char) + 2*sizeof(int)+ (recToComp+cr+1)*(s1+s2),s1);
                                if(v2 != key3) break;
                                cr++;
                        }
                        while(cl != recToComp){
                                memcpy(&v2,data + sizeof(char) + 2*sizeof(int)+ (recToComp-cl-1)*(s1+s2),s1);
                                if(v2 != key3) break;
                                cl++;
                        }
		}

		if(cr>=cl || (cr==cl-1 && (flag==0 || flag==2))){//then <cl> goes to right
			temp_counter = recToComp + cl;
	                temp = malloc(temp_counter*(s1+s2));
			memcpy(temp,data + sizeof(char) + 2*sizeof(int) + (recToComp-cl)*(s1+s2),temp_counter*(s1+s2));
			temp_counter = recToComp - cl;
			memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
		}
		else{
			temp = malloc((recToComp-cr-1)*(s1+s2));
			memcpy(temp,data + sizeof(char) + 2*sizeof(int) + (recToComp+cr+1)*(s1+s2),(recToComp-cr-1)*(s1+s2));
			temp_counter = recToComp + cr + 1;
			memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
			if(flag==2) insertInDataBlock(fd,blockNum,value1,value2);
		}
                memcpy(&next,data + sizeof(char),sizeof(int));
                BF_Block_SetDirty(block);
                if(BF_UnpinBlock(block) != BF_OK)
                        BF_PrintError(BF_ERROR);
                if(BF_AllocateDataBlock(fd,block,value1,value2,0) != BF_OK)
                        return AME_ERROR;
                if(BF_GetBlockCounter(AM_File[fd]->fileDesc,&newBlockNum) != BF_OK){
			AM_errno = AME_FULL_MEMORY_ERROR;
                        return AME_ERROR;
		}
                if(setNext(fd,blockNum,newBlockNum-1) == AME_ERROR)
			return AME_ERROR;
                data = BF_Block_GetData(block);
		if(cr>=cl || (cr==cl-1 && (flag==0 || flag==2))){
			temp_counter = recToComp + cl;
			memcpy(data + sizeof(char) + 2*sizeof(int),temp,temp_counter*(s1+s2));
			memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
			if(flag==2){
				if(insertInDataBlock(fd,newBlockNum-1,value1,value2) == AME_ERROR)
					return AME_ERROR;
			}
		}
		else{
			temp_counter = recToComp - cr - 1;
			memcpy(data + sizeof(char) + 2*sizeof(int),temp,temp_counter*(s1+s2));
			memcpy(data + sizeof(char) + sizeof(int),&temp_counter,sizeof(int));
		}
		memcpy(data + sizeof(char),&next,sizeof(int));
                BF_Block_SetDirty(block);
                if(BF_UnpinBlock(block) != BF_OK)
                        BF_PrintError(BF_ERROR);

		if(flag==1){
			if(insertInDataBlock(fd,blockNum,value1,value2) == AME_ERROR) return AME_ERROR;
		}
		else if(flag==0){
			if(insertInDataBlock(fd,newBlockNum-1,value1,value2) == AME_ERROR) return AME_ERROR;
		}
	}

        if(BF_GetBlock(AM_File[fd]->fileDesc,newBlockNum-1,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);

	memcpy(value,data + sizeof(char) + 2*sizeof(int),s1);
        BF_Block_SetDirty(block);
        if(BF_UnpinBlock(block) != BF_OK)
                BF_PrintError(BF_ERROR);

	free(temp);
	if(AM_File[fd]->max_data_records % 2)
		free(temp2);
        BF_Block_Destroy(&block);

	return newBlockNum-1;
}

int insertInIndexBlock(int fd,int blockToInsert,void* value1,int blockP){
        BF_Block *block;
        BF_Block_Init(&block);
        int counter;
        int counter2;
        int s1=AM_File[fd]->size1, s2=sizeof(int);//s2 = pointer's size
        char* data;
	char id;
        char key1[AM_File[fd]->size1];
        int key2;
        float key3;
        int v1;
        float v2;

        if(BF_GetBlock(AM_File[fd]->fileDesc,blockToInsert,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
	memcpy(&id,data,sizeof(char));
	if(id != AM_INDEX_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
		return AME_ERROR;//wrong blockNum
	}
        memcpy(&counter,data + sizeof(char) + sizeof(int),sizeof(int));
        counter2 = 0;
        while(counter2 != counter){
                if(AM_File[fd]->type1 == STRING){
                        memcpy(key1,data + sizeof(char) + 3*sizeof(int)+ counter2*(s1+s2),s1);
                        if(isGreaterString(value1,key1)) break;
                }
                else if(AM_File[fd]->type1 == INTEGER){
                        memcpy(&key2,data + sizeof(char) + 3*sizeof(int)+ counter2*(s1+s2),sizeof(int));
                        memcpy(&v1,value1,sizeof(int));
                        if(isGreaterInteger(v1,key2)) break;
                }
                else{
                        memcpy(&key3,data + sizeof(char) + 3*sizeof(int)+ counter2*(s1+s2),sizeof(int));
                        memcpy(&v2,value1,sizeof(float));
                        if(isGreaterFloat(v2,key3)) break;
                }
                counter2++;
        }
        char* temp = malloc((counter - counter2)*(s1+s2)*sizeof(char));
        memcpy(temp,data + sizeof(char) + 3*sizeof(int) + counter2*(s1+s2),((counter-counter2)*(s1+s2))*sizeof(char));
        memcpy(data + sizeof(char) + 3*sizeof(int) + counter2*(s1+s2),value1,s1);
        memcpy(data + sizeof(char) + 3*sizeof(int) + counter2*(s1+s2) + s1,&blockP,s2);
        memcpy(data + sizeof(char) + 3*sizeof(int) + (counter2+1)*(s1+s2),temp,((counter-counter2)*(s1+s2))*sizeof(char));
	counter++;
        memcpy(data + sizeof(char) + sizeof(int),&counter,sizeof(int));
        BF_Block_SetDirty(block);
        if(BF_UnpinBlock(block) != BF_OK)
                BF_PrintError(BF_ERROR);
        free(temp);
        BF_Block_Destroy(&block);

        return 1;
}

int indexBlockSplit(int fd, int blockNum, void* value,void* newValue,int blockP){
        BF_Block *block;
        BF_Block_Init(&block);
        int s1=AM_File[fd]->size1, s2=AM_File[fd]->size2;
        char* data;
	char id;
        int recToComp,recToComp2;
        char key1[s1];
        int key2,v2;
        float key3,v3;
        int flag = 0;
	int parent;
        char* temp;
        int newBlockNum;
        if(BF_GetBlock(AM_File[fd]->fileDesc,blockNum,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
	memcpy(&id,data,sizeof(char));
	if(id != AM_INDEX_ID){
		AM_errno = AME_WRONG_BLOCK_NUMBER;
		return AME_ERROR;//wrong blockNum
	}
        if(!(AM_File[fd]->max_data_records % 2)){//block has even num of records
                recToComp = AM_File[fd]->max_index_records/2;//starts from 0 to max-1
		recToComp2 = recToComp +1;
                if(AM_File[fd]->type1 == STRING){
                        memcpy(key1,data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),s1);
                        if(!isGreaterString(value,key1)) flag = 1;//our key > data[max_rec/2]--->key->up
			memcpy(key1,data + sizeof(char) + 3*sizeof(int)+ recToComp2*(s1+s2),s1);
			if(!isGreaterString(value,key1)) flag = 2;//our key > data[max_rec/2+1]--->data[max_rec/2+1]->up
                }
                else if(AM_File[fd]->type1 == INTEGER){
                        memcpy((char*)&key2,data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&v2,value,sizeof(int));
                        if(!isGreaterInteger(v2,key2)) flag = 1;
			memcpy((char*)&key2,data + sizeof(char) + 3*sizeof(int)+ recToComp2*(s1+s2),s1);
			memcpy(&v2,value,sizeof(int));
			if(!isGreaterInteger(v2,key2)) flag = 2;
                }
                else{
                        memcpy((char*)&key3,data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&v3,value,sizeof(float));
                        if(!isGreaterFloat(v3,key3)) flag = 1;
			memcpy((char*)&key3,data + sizeof(char) + 3*sizeof(int)+ recToComp2*(s1+s2),s1);
			memcpy(&v3,value,sizeof(float));
			if(!isGreaterFloat(v3,key3)) flag = 2;
                }
                if(flag==0){//flag=0 [1,recToComp] stays here
                        temp = malloc(recToComp*(s1+s2) + s2);
                        memcpy(temp,data + sizeof(char) + 2*sizeof(int)+ recToComp*(s1+s2),recToComp*(s1+s2)+s2);
                        memcpy(newValue,data + sizeof(char) + 3*sizeof(int)+ (recToComp-1)*(s1+s2),s1);
			memcpy(&parent,data + sizeof(char),sizeof(int));
			recToComp--;
                        memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));
                        BF_Block_SetDirty(block);

                        if(BF_UnpinBlock(block) != BF_OK)
                                BF_PrintError(BF_ERROR);
                        if(BF_AllocateIndexBlock(fd,block,value,0,parent) != BF_OK)
                                return AME_ERROR;
                        if(BF_GetBlockCounter(AM_File[fd]->fileDesc,&newBlockNum) != BF_OK){
				AM_errno = AME_INVALID_FILE_ERROR;
                                return AME_ERROR;
			}
                        data = BF_Block_GetData(block);
                        recToComp++;
                        memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));
                        memcpy(data + sizeof(char) + 2*sizeof(int), temp, recToComp*(s1+s2)+s2);

                        if(insertInIndexBlock(fd,blockNum,value,blockP) == AME_ERROR) return AME_ERROR;
                }
                else if(flag==1){
                        temp = malloc(recToComp*(s1+s2));
                        memcpy(temp,data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),recToComp*(s1+s2));
                        memcpy(newValue,value,s1);
                        memcpy(&parent,data + sizeof(char),sizeof(int));
                        memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));
                        BF_Block_SetDirty(block);

                        if(BF_UnpinBlock(block) != BF_OK)
                                BF_PrintError(BF_ERROR);
                        if(BF_AllocateIndexBlock(fd,block,value,0,parent) != BF_OK)
                                return AME_ERROR;
                        if(BF_GetBlockCounter(AM_File[fd]->fileDesc,&newBlockNum) != BF_OK){
				AM_errno = AME_INVALID_FILE_ERROR;
                                return AME_ERROR;
			}
                        data = BF_Block_GetData(block);
                        memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));
			memcpy(data + sizeof(char) + 2*sizeof(int),&blockP,s2);
                        memcpy(data + sizeof(char) + 3*sizeof(int), temp, recToComp*(s1+s2));
                }
		else if(flag==2){
                        temp = malloc((recToComp-1)*(s1+s2) + s2);
                        memcpy(temp,data + sizeof(char) + 2*sizeof(int)+ (recToComp+1)*(s1+s2),(recToComp - 1)*(s1+s2)+s2);
                        memcpy(newValue,data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&parent,data + sizeof(char),sizeof(int));
                        memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));
                        BF_Block_SetDirty(block);

                        if(BF_UnpinBlock(block) != BF_OK)
                                BF_PrintError(BF_ERROR);
                        if(BF_AllocateIndexBlock(fd,block,value,0,parent) != BF_OK)
                                return AME_ERROR;
                        if(BF_GetBlockCounter(AM_File[fd]->fileDesc,&newBlockNum) != BF_OK){
				AM_errno = AME_INVALID_FILE_ERROR;
                                return AME_ERROR;
			}
                        data = BF_Block_GetData(block);
                        recToComp--;
                        memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));
                        memcpy(data + sizeof(char) + 2*sizeof(int), temp, recToComp*(s1+s2)+s2);

                        if(insertInIndexBlock(fd,newBlockNum-1,value,blockP) == AME_ERROR) return AME_ERROR;
                }
        }
        else{//odd
                recToComp = AM_File[fd]->max_index_records/2 + 1;//starts from 0 to max-1
                if(AM_File[fd]->type1 == STRING){
                        memcpy(key1,data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),s1);
                        if(!isGreaterString(value,key1)) flag = 1;
                }
                else if(AM_File[fd]->type1 == INTEGER){
                        memcpy((char*)&key2,data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&v2,value,sizeof(int));
                        if(!isGreaterInteger(v2,key2)) flag = 1;
                }
                else{
                        memcpy((char*)&key3,data + sizeof(char) + 3*sizeof(int)+ recToComp*(s1+s2),s1);
                        memcpy(&v3,value,sizeof(float));
                        if(!isGreaterFloat(v3,key3)) flag = 1;
                }
                recToComp--;
                temp = malloc(recToComp*(s1+s2) + s2);
                memcpy(temp,data + sizeof(char) + 2*sizeof(int) + (recToComp+1)*(s1+s2),recToComp*(s1+s2)+s2);
                memcpy(newValue,data + sizeof(char) + 3*sizeof(int) + recToComp*(s1+s2),s1);
		memcpy(&parent,data + sizeof(char),sizeof(int));
                memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));//
                BF_Block_SetDirty(block);

                if(BF_UnpinBlock(block) != BF_OK)
                        BF_PrintError(BF_ERROR);
                if(BF_AllocateIndexBlock(fd,block,value,0,-1) != BF_OK)
                        return AME_ERROR;
                if(BF_GetBlockCounter(AM_File[fd]->fileDesc,&newBlockNum) != BF_OK){
			AM_errno = AME_INVALID_FILE_ERROR;
                        return AME_ERROR;
		}
                data = BF_Block_GetData(block);
                memcpy(data + sizeof(char) + sizeof(int),&recToComp,sizeof(int));
                memcpy(data + sizeof(char) + 2*sizeof(int),temp,recToComp*(s1+s2)+s2);
                if(flag==0)
                        if(insertInIndexBlock(fd,blockNum,value,blockP) == AME_ERROR) return AME_ERROR;
                else
                        if(insertInIndexBlock(fd,newBlockNum-1,value,blockP) == AME_ERROR) return AME_ERROR;
        }

        BF_Block_SetDirty(block);
        if(BF_UnpinBlock(block) != BF_OK)
                BF_PrintError(BF_ERROR);

	free(temp);
        BF_Block_Destroy(&block);
        return newBlockNum-1;
}



int insertInNotFull(int fd,int parent,void* value,int blockP){
        BF_Block *block;
        BF_Block_Init(&block);
        int counter;
        int counter2;
        int s1=AM_File[fd]->size1, s2=sizeof(int);//s2 = pointer's size
        char* data;
	int newBlock;
        int v1;
        float v2;
	void* newValue;
	if(AM_File[fd]->type1==STRING)
		newValue = malloc(s1*sizeof(char));
	else
		newValue = malloc(64);
	int root = AM_File[fd]->root;
	do{
		if(newBlock = indexBlockSplit(fd,parent,value,newValue,blockP) == AME_ERROR) return AME_ERROR;
		blockP = newBlock;
		memcpy(value,newValue,s1);
	        if(BF_GetBlock(AM_File[fd]->fileDesc,parent,block) != BF_OK){
			AM_errno = AME_FULL_MEMORY_ERROR;
	                return AME_ERROR;
		}
        	data = BF_Block_GetData(block);
        	memcpy(&parent,data + sizeof(char),sizeof(int));
		if(BF_UnpinBlock(block) != BF_OK)
	                BF_PrintError(BF_ERROR);
	}while((parent!=-1) && (!isFullIndexBlock(fd,parent)));

	if(parent!=-1)
		if(insertInIndexBlock(fd,parent,value,blockP) == AME_ERROR) return AME_ERROR;
	else{//we have split the root
		if(BF_AllocateIndexBlock(fd,block,value,1,-1) != BF_OK)
			return AME_ERROR;
                data = BF_Block_GetData(block);
                memcpy(data + sizeof(char) + 2*sizeof(int),&root,s2);//left p =previous root
		memcpy(data + sizeof(char) + 2*sizeof(int) + s1,&blockP,s2);//right = the new block
		BF_Block_SetDirty(block);
		if(BF_UnpinBlock(block) != BF_OK)
			BF_PrintError(BF_ERROR);
	}

	free(newValue);
        BF_Block_Destroy(&block);
	return AME_OK;
}

//*******************************************SCAN FUNCTIONS********************************************//
int firstDataBlock(int fd){
	BF_Block *block;
	BF_Block_Init(&block);
	char id;
	int root;
	int blockNum;
	char* data;
	//first get the root from metadata
        if(BF_GetBlock(fd,0,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
	memcpy(&root,data + 3*sizeof(char) + 2*sizeof(int), sizeof(int));
	if(BF_UnpinBlock(block) != BF_OK)
                BF_PrintError(BF_ERROR);
	blockNum = root;
	if(BF_GetBlock(fd,blockNum,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
		return AME_ERROR;
	}
	data = BF_Block_GetData(block);
	memcpy(&id,data,sizeof(char));
	while(id != AM_DATA_ID){
		memcpy(&blockNum,data + sizeof(char) + 2*sizeof(int),sizeof(int));//save left block
		if(BF_UnpinBlock(block) != BF_OK)
			BF_PrintError(BF_ERROR);
		if(BF_GetBlock(fd,blockNum,block) != BF_OK){
			AM_errno = AME_FULL_MEMORY_ERROR;
			return AME_ERROR;
		}
		data = BF_Block_GetData(block);
        	memcpy(&id,data,sizeof(char));
	}
	memcpy(AM_Scan[fd]->value2,data + sizeof(char) + 2*sizeof(int),AM_Scan[fd]->size1);
	if(BF_UnpinBlock(block) != BF_OK)
		BF_PrintError(BF_ERROR);

	BF_Block_Destroy(&block);

	return blockNum;
}

int findRecord(int fd){
        BF_Block *block;
        BF_Block_Init(&block);
        char* data;
	char id;
	int s1=AM_Scan[fd]->size1,s2=AM_Scan[fd]->size2;
	int total_rec;
        char key1[s1];
        int key2,v1;
        float key3,v2;

	if(BF_GetBlock(fd,AM_Scan[fd]->Current_block_num,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return AME_ERROR;
	}
        data = BF_Block_GetData(block);
	memcpy(&total_rec,data + sizeof(char) + sizeof(int),sizeof(int));
	int counter=0;
	while(counter != total_rec){
                if(AM_Scan[fd]->type1 == STRING){
                        memcpy(key1,data + sizeof(char) + 2*sizeof(int)+ counter*(s1+s2),s1);
                        if(isGreaterString(AM_Scan[fd]->value1,key1) || !strcmp(AM_Scan[fd]->value1,key1)) break;
                }
                else if(AM_Scan[fd]->type1 == INTEGER){
                        memcpy(&key2,data + sizeof(char) + 2*sizeof(int)+ counter*(s1+s2),sizeof(int));
                        memcpy(&v1,AM_Scan[fd]->value1,sizeof(int));
                        if(isGreaterInteger(v1,key2) || v1==key2) break;
                }
                else{
                        memcpy(&key3,data + sizeof(char) + 2*sizeof(int)+ counter*(s1+s2),sizeof(int));
			memcpy(&v2,AM_Scan[fd]->value1,sizeof(float));
                        if(isGreaterFloat(v2,key3) || v2==key3) break;
                }
		counter++;
	}
	if(counter == total_rec){
		counter=0;
		//get next block number
		memcpy(&(AM_Scan[fd]->Current_block_num),data + sizeof(char),sizeof(int));
	}
	if(BF_UnpinBlock(block) != BF_OK)
                BF_PrintError(BF_ERROR);

        BF_Block_Destroy(&block);

	return counter;
}

void findNextRecord(int fd,int flag){
	BF_Block *block;
        BF_Block_Init(&block);
        char* data;
        char id;
	int total_rec;
	int* iv1;
	int* iv2;
	float* fv1;
	float* fv2;
	int s1=AM_Scan[fd]->size1,s2=AM_Scan[fd]->size2;
        if(BF_GetBlock(fd,AM_Scan[fd]->Current_block_num,block) != BF_OK){
		AM_errno = AME_FULL_MEMORY_ERROR;
                return ;
	}
        data = BF_Block_GetData(block);
        memcpy(&id,data,sizeof(char));
	memcpy(&total_rec,data + sizeof(char) + sizeof(int),sizeof(int));
	if(AM_Scan[fd]->Current_record_num == total_rec){
		memcpy(&(AM_Scan[fd]->Current_block_num),data + sizeof(char),sizeof(int));
		if(AM_Scan[fd]->Current_block_num == -1){
			AM_errno = AME_EOF;
			return ;
		}
		if(BF_UnpinBlock(block) != BF_OK)
			BF_PrintError(BF_ERROR);
		if(BF_GetBlock(fd,AM_Scan[fd]->Current_block_num,block) != BF_OK){
			AM_errno = AME_FULL_MEMORY_ERROR;
			return ;
		}
		data = BF_Block_GetData(block);
		AM_Scan[fd]->Current_record_num = 0;
	}
	if(flag == 0){
		memcpy(AM_Scan[fd]->value2,data + sizeof(char) + 2*sizeof(int)+ AM_Scan[fd]->Current_record_num*(s1+s2),s1);
		memcpy(AM_Scan[fd]->value3,data + sizeof(char) + 2*sizeof(int)+ AM_Scan[fd]->Current_record_num*(s1+s2)+s1,s2);
	}
	else if(flag == 1){
		while(AM_Scan[fd]->Current_record_num != total_rec){
			memcpy(AM_Scan[fd]->value2,data + sizeof(char) + 2*sizeof(int)+ AM_Scan[fd]->Current_record_num*(s1+s2),s1);
			memcpy(AM_Scan[fd]->value3,data + sizeof(char) + 2*sizeof(int)+ AM_Scan[fd]->Current_record_num*(s1+s2)+s1,s2);
			if(AM_Scan[fd]->type1 == STRING){
				if(strcmp(AM_Scan[fd]->value1,AM_Scan[fd]->value2)) break;
			}
			else if(AM_Scan[fd]->type1 == INTEGER){
				iv1 = (int*)AM_Scan[fd]->value1;
				iv2 = (int*)AM_Scan[fd]->value2;
				if(*iv1 != *iv2) break;
			}
			else{
				fv1 = (float*)AM_Scan[fd]->value1;
                                fv2 = (float*)AM_Scan[fd]->value2;
                                if(*fv1 != *fv2) break;
			}
			AM_Scan[fd]->Current_record_num++;
		}
		if(AM_Scan[fd]->Current_record_num == total_rec){
	                memcpy(&(AM_Scan[fd]->Current_block_num),data + sizeof(char),sizeof(int));
	                if(AM_Scan[fd]->Current_block_num == -1){
	                        AM_errno = AME_EOF;
	                        return ;
        	        }
			if(BF_UnpinBlock(block) != BF_OK)
	                        BF_PrintError(BF_ERROR);
	                if(BF_GetBlock(fd,AM_Scan[fd]->Current_block_num,block) != BF_OK){
				AM_errno = AME_FULL_MEMORY_ERROR;
	                        return ;
			}
	                data = BF_Block_GetData(block);
	                AM_Scan[fd]->Current_record_num = 0;
			memcpy(AM_Scan[fd]->value2,data + sizeof(char) + 2*sizeof(int),s1);
			memcpy(AM_Scan[fd]->value3,data + sizeof(char) + 2*sizeof(int)+s1,s2);
		}
	}
	AM_Scan[fd]->Current_record_num++;
	if(BF_UnpinBlock(block) != BF_OK)
		BF_PrintError(BF_ERROR);

	BF_Block_Destroy(&block);
}
