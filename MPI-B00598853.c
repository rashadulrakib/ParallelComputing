/*
Author: Md Rashadul Hasan Rakib: B00598853
This is an in-place distributed memory program using MPI
Input: 
	number of processors
	An array of elements evenly distributed over the processors
	Index of the ith smallest element within the array
Output:
	select the ith smallest value
*/
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#define TOTAL_ELEMENTS 2000000000 //2 Billion data
#define ITH_ELEMENT TOTAL_ELEMENTS/2
#define MAX_PROCS 32 //Maximum number of processors

// Creates an array of random numbers
int *create_rand_nums(int proc, int ELEMENTS_PER_PROC, int maxVAl) {
	int *rand_nums= (int *)calloc(ELEMENTS_PER_PROC, sizeof(int)); //allocate memory for each processor
    int i;
    for (i = 0; i < ELEMENTS_PER_PROC; i++) {
        rand_nums[i] = rand() % maxVAl; //generate number between 0 to maxVAl
    }
    return rand_nums;
}

//generate random number within a range
int getRandomNumber(int min, int max){
	return (rand() % (max-min+1))+min;
}

//select the processor with smallest index which has some data
int getRandomProcessor(int activeProcFlags[MAX_PROCS], int world_size){
	//activeProcFlags[MAX_PROCS]: store the flags for the processors (active/inactive)
								//If a processor has some data, it is active and vice versa
	//world_size: number of predecessors used in the MPI communication world
	int m=0;
	for(m=0;m<world_size;m++){
		if(activeProcFlags[m]>=0){ //If a processor has at least 1 data, the flag = 0; 
		//flag value=index difference(max index - min index) of the data block 
			return m; //return the processor with smallest index
		}
	}
}

//swap two values
void swap(int *i, int *j) {
    int t = *i;
    *i = *j;
    *j = t;
}

void main(int argc, char** argv) {
	MPI_Init(&argc, &argv); //Initialize the MPI execution environment
		
	int world_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank); //determine the rank of the processor: who I am
	int world_size;
	MPI_Comm_size(MPI_COMM_WORLD, &world_size); //how many processors are in the execution environment
	
	int iGlobal=TOTAL_ELEMENTS/2; //index of the median element; and we are looking for that ith smallest element
	int ELEMENTS_PER_PROC=TOTAL_ELEMENTS/world_size;
	int * rand_nums = create_rand_nums(world_rank,ELEMENTS_PER_PROC, TOTAL_ELEMENTS); //generate random numbers in each processor
	MPI_Barrier(MPI_COMM_WORLD); //Blocks until all processes in the communicator have reached this routine.
	
	int randomProc; //index of the randomly selected proc within the MPI environment
	int randomValue; //random pivot value
	int kGlobal; //the global distance of the random pivot value within the whole data block
	int q; //index of the pivot value
	int k; //the local distance of the random pivot value within the local data block
	int left; //left pointer
	int right; //right pointer
	int isPivotExist; //determine whether a proc contains the pivot value or not
	int activeProcFlags[MAX_PROCS]; //store flags for the active/inactive processor
									//a proc is active, if it has at least one data item
	int indexDiff; //difference of two index
			
	int p=0; //initial local left index of local data block
	int r=ELEMENTS_PER_PROC-1; //initial local right index of local data block
	indexDiff=r-p; //if a proc has no data item, indexDiff is negative  
	
	//gather all indexDiff to master processor (proc index =0) into activeProcFlags
	MPI_Gather(&indexDiff, 1, MPI_INT, activeProcFlags, 1, MPI_INT, 0, MPI_COMM_WORLD);

	// Seed the random number generator to get different results each time
	srand(world_rank);
	
	double t;
	double elapsedTime; //elapsed time to select the ith smallest item
	double totalTime=0; //store the elapsed times of all processors that are working to select the ith smallest item  

	t = MPI_Wtime(); //start the timer
		
	while (1){
		left = p; 
		right = r;
		isPivotExist=0;
		kGlobal=0;
			
		if(world_rank == 0){ 
			//If I am master select the active proc with smallest id
			randomProc = getRandomProcessor(activeProcFlags, world_size);
		}
			
		MPI_Bcast(&randomProc, 1, MPI_INT, 0, MPI_COMM_WORLD); //Broadcast the random proc id to all procs
	
		if(world_rank==randomProc){
			//If I am the random proc select a random pivot value within my local data from local p to r
			randomValue = rand_nums[getRandomNumber(p,r)];
		}
	
		//Broadcast random value to all processors from the random proc
		MPI_Bcast(&randomValue, 1, MPI_INT, randomProc, MPI_COMM_WORLD);
	
		//partition the local data using the global pivot such that left values < pivot and right values>=pivot 
		while(left<=right){
			while(left<=r && rand_nums[left]<randomValue){ //traverse from left to right
				//as long long as the values are less than pivot  
				left++; //increase left pointer
			}
			while(right>=p && rand_nums[right]>=randomValue){ //traverse from right to left
				//as long long as the values are greater than or equal to pivot
				if(rand_nums[right]==randomValue){ 
					isPivotExist=1; //if the local data block contains the global pivot,isPivotExist=1 
				}
				right--; //decrease right pointer
			}
			if(left<right){
				swap(&rand_nums[left],&rand_nums[right]); //swap two values when left < right
			}
		}
	
		q=left; //partition index of global pivot
		k=q-p; // distance of pivot prom left to right
			
		if(world_rank == 0){
			k=k+1; //If I am master, add 1 to distance because item at index 0 refers to the first element and so on
		}
		
		//Sum all the local distance to global distance and broad cast the global distance to all procs
		MPI_Allreduce(&k, &kGlobal, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

		if(iGlobal==kGlobal){
			printf("-------------------- %dth smallest value=%d ---------------\n",ITH_ELEMENT, randomValue);
			break;
		}else if(iGlobal < kGlobal){ // index of global ith smallest < global index of the pivot 
			r=q-1; 
		}else{ // index of global ith smallest > global index of the pivot 
			if(isPivotExist==1 && randomProc==world_rank){
				p=q+1; //If I am the proc who selects the random value, shift my left pointer by 1 
			}else{
				p=q; //If I am not the proc who selects the random value, do not shift my left pointer by 1 
			}
			iGlobal = iGlobal-kGlobal; //change the global distance
		}
			
		indexDiff=r-p; //if a proc has no data item, indexDiff is negative  
		
		//gather all indexDiff to master processor into activeProcFlags (proc index =0)
		MPI_Gather(&indexDiff, 1, MPI_INT, activeProcFlags, 1, MPI_INT, 0, MPI_COMM_WORLD);
	}

	MPI_Barrier(MPI_COMM_WORLD); //Blocks until all processes in the communicator have reached this routine.
	t = MPI_Wtime() - t; //calculate the time difference for each proc

	//sum all individual time differences into master proc
	MPI_Reduce(&t, &totalTime, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

	if(world_rank==0){
		//If I am master, calculate the average of the elapsed times
		elapsedTime = totalTime/(double)world_size;
		printf ("elapsed time = %f seconds\n",elapsedTime);
	}
			
	// free the dynamically allocated memory
	free(rand_nums);
		
	MPI_Barrier(MPI_COMM_WORLD); //Blocks until all processes in the communicator have reached this routine.
	MPI_Finalize(); //Terminates MPI execution environment
}
