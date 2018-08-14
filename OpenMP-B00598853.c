/*
Author: Md Rashadul Hasan Rakib: B00598853
This is an in-place shared memory program using OpenMP following the Strided algorithm with block size 1
This is the parallel version of the modified sequential program that selects the ith smallest value within an array.
The modification has been done to handle the duplicate items so as to keep the time complexity O(N)
Input: 
	An array of elements
	Index of the ith smallest element within the array
Output:
	select the ith smallest value
*/

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <math.h>
#include<limits.h>
#define TOTAL_ELEMENTS 2000000000 //2 billion items
#define MAX_VALUE TOTAL_ELEMENTS //generate random numbers within 0 to MAX_VALUE-1
#define TOTAL_THREADS 16 //number of parallel threads
#define ITH_ELEMENT TOTAL_ELEMENTS/2 //index of the median element

//generate random number within a range
int getRandomNumber(int min, int max){
	return (rand() % (max - min + 1)) + min;
}

//swap two values
void swap(int *ii, int *jj) {
	int t = *ii;
	*ii = *jj;
	*jj = t;
}

//this function traverse in the left direction within p to r index as long as it finds consecutive duplicate pivot values
int travarseLeftDuplicates(int * A, int p, int r, int i, int pivot){
	int m;
	int q = r + 1; //keep the original partition position of the pivot element
	for (m = r; m >= p; m--){ //traversing in the left direction within the block
		if (A[m] == pivot && (m - p + 1) >= i){ //As long as the pivot item is same as the item in the left direction 
			//and the distance I traverse (in left direction) 					
			//is greater than or equal to the distance of the ith element.
			q = m;	//update the index of the partition as long as we traverse 
			if ((m - p + 1) == i){ // distance of the ith element and mth element same
				return -1; // -1 indicates that we find the ith smallest item
			}
		}
		else{
			break; //the pivot is not same in the left direction or the distance I traverse 				
			//(in left direction) is less than the distance of the ith element.
		}
	}

	return q; //return the index of the item which is not same as pivot
}

//this function traverse in the right direction within p to r index as long as it finds consecutive duplicate pivot values
int travarseRightDuplicates(int * A, int p, int r, int i, int pivot){
	int m;
	int q = p - 1; //keep the original partition position of the pivot element
	for (m = p; m <= r; m++){ //traversing in the right direction within the block
		if (A[m] == pivot && (m - p + 1) <= i){ //As long as the pivot item is same as the item 				
			//in the right direction and distance I traverse (in right direction) is less 				
			//than or equal to the distance of the ith element.
			q = m; //update the index of the partition as long as we traverse
			if ((m - p + 1) == i){ // distance of the ith element and mth element same
				return -1; // -1 indicates that we find the ith smallest item
			}
		}
		else{
			break; //the pivot is not same in the right direction or the distance I traverse 				
			//(in right direction) is greater than the distance of the ith element.
		}
	}

	return q; //return the index of the item which is not same as pivot
}


//this function returns the partition index of a random item within p to r
int partition(int * A, int p, int r){
	int x = A[r];
	int i = p - 1;
	int j;
	int temp;
	for (j = p; j <= (r - 1); j++){
		if (A[j] <= x){
			i = i + 1;
			swap(&A[i], &A[j]);
		}
	}
	swap(&A[i + 1], &A[r]);
	return i + 1; //return the index
}

// this function call the partition function for a random value
int randomizedPartition(int * A, int p, int r){
	int i = rand() % (r - p + 1) + p; //select a random index within p to r
	swap(&A[r], &A[i]); //swap the ith value with last value in r
	return partition(A, p, r);
}

int randomizedSelect(int * A, int p, int r, int i){
	//A: Array of the items
	//p: left index of the data block within which I find the ith smallest item
	//r: right index of the data block within which I find the ith smallest item
	//i: index of the ith smallest item: which can be considered as the distance from left position

	if (p == r){ //when one item within the block
		return A[p];
	}
	int q = randomizedPartition(A, p, r); //index of the pivot within p to r of A such that, left items <pivot : right items >= pivot
	int k = q - p + 1; //distance of pivot from the left position within the block
	if (i == k){ //distance of the ith element = distance of pivot: pivot value is the answer
		return A[q];
	}
	else if (i<k){ //distance of the ith element < distance of pivot  
		int pivot = A[q];
		q = travarseLeftDuplicates(A, p, q - 1, i, pivot); //traverse left direction to find the consecutive duplicates
		if (q == -1){ //q=-1 indicates that we find the ith element during the left traverse and we return the ith element 
			return pivot;
		}
		else{
			return randomizedSelect(A, p, q - 1, i); //recursive call on the left part of A
		}
	}
	else{
		int pivot = A[q];
		q = travarseRightDuplicates(A, q + 1, r, i - k, pivot); //traverse right direction to find the consecutive duplicates
		if (q == -1){ //q=-1 indicates that we find the ith element during the right traverse and we return the ith element
			return pivot;
		}
		else{
			return randomizedSelect(A, q + 1, r, i - k); //recursive call on the right part of A
		}
	}
	return 0;
}

void callRandomSelection(int * A,int p,int r,int i){
	int nthreads = TOTAL_THREADS;
	int tid;
	int l;
	int left, right;

	while (p < r && (r - p)>=2 * TOTAL_THREADS){ //(r - p)>=2 * TOTAL_THREADS: to ensure that we have at least 2*TOTAL_THREADS data items
													// for parallel partitioning. Value of 2*TOTAL_THREADS is very small w.r.t total data 
		int pivot = A[getRandomNumber(p, r)]; //select global pivot within p to r
	
		int minLeft = INT_MAX; //minLeft stores the minimum of all left pointers.
								// a single thread generates two pointers while partitioning its own data
		int maxRight = -1; //maxRight stores the maximum of all right pointers 
#pragma omp parallel num_threads(nthreads) private(left,right,tid) 
		{
			tid = omp_get_thread_num(); //get the thread id

			left = tid + p; //initialize the left pointer by adding tid + p (offset)
			int items = r - p + 1; //total items
			
			// calculate the initial right pointer for each thread
			if (items%TOTAL_THREADS != 0){ 
				right = (items / TOTAL_THREADS) * TOTAL_THREADS + p + tid;
				if (right > r){
					right = ((items / TOTAL_THREADS) - 1)*TOTAL_THREADS + p + tid;
				}
			}
			else{
				right = ((items / TOTAL_THREADS) - 1)*TOTAL_THREADS + p + tid;
			}
			
			int llimit = left;
			int rlimit = right;

			//partition the local data using the global pivot such that left values < pivot and right values>=pivot
			while (left <= right){ //traverse from left to right
				//as long long as the values are less than pivot 
				while (left <= rlimit && A[left]< pivot){
					left = left + nthreads; //increase left pointer
				}

				while (right >= llimit && A[right] >= pivot){ //traverse from right to left
				//as long long as the values are greater than or equal to pivot
					right = right - nthreads; //decrease right pointer
				}
				if (left<right){
					swap(&A[left], &A[right]);
				}
			}

#pragma omp critical //only single thread will execute this block at a time
			{
				if (minLeft>left){
					minLeft = left;
				}
				if (maxRight < right){
					maxRight = right;
				}
			}

		}

		left = minLeft;
		right =  maxRight;

		//Clean-up phase: partition the dirty block, this will give us the final index of the pivot value 
		while (left <= right){
			while (left <= r && A[left]< pivot){
				left = left + 1;
			}

			while (right >= p && A[right] >= pivot){
				right = right - 1;
			}
			if (left<right){
				swap(&A[left], &A[right]);
			}
		}

		int q = left;
		int k = q - p + 1;
	
		if (i == k){
			printf("-------------------- %dth smallest value=%d ---------------\n", ITH_ELEMENT, pivot);
			break;
		}
		else if (i<k){
			q = travarseLeftDuplicates(A, p, q - 1, i, pivot); //traverse left direction to find the consecutive duplicates
			if (q == -1){ //q=-1 indicates that we find the ith element during the left traverse and we return the ith element 
				printf("-------------------- %dth smallest value=%d ---------------\n", ITH_ELEMENT, pivot);
				break;
			}
			r = q - 1;
		}
		else{
			q = travarseRightDuplicates(A, q + 1, r, i - k, pivot); //traverse right direction to find the consecutive duplicates
			if (q == -1){ //q=-1 indicates that we find the ith element during the right traverse and we return the ith element
				printf("-------------------- %dth smallest value=%d ---------------\n", ITH_ELEMENT, pivot);
				break;
			}
			p = q + 1;
			i = i - k;
		}
	}

	if (p<=r){ //when the ith smallest value is not selected parallel, then we call the sequential randomized selection. 
	//the value of p and r is generally very small
		int item = randomizedSelect(A, p, r, i);
		printf("-------------------- %dth smallest value=%d ---------------\n", ITH_ELEMENT, item);
	}
}

void main() {

	int p=0; //initial left index
	int r=TOTAL_ELEMENTS-1; //initial right index
	int i=ITH_ELEMENT; //index of the ith element
	int l;
		
	int * A =(int *)calloc(TOTAL_ELEMENTS, sizeof(int)); //allocate the memory for total items
	
	// Seed the random number generator to get different results each time
	srand((unsigned)time(0));
	for (l = 0; l<TOTAL_ELEMENTS; l++){
		A[l] = rand() % MAX_VALUE;
	}
	
	//start the timer
	double t = omp_get_wtime();
				
	callRandomSelection(A,p,r,i); //call the callRandomSelection function to select the ith smallest value
	
	t = omp_get_wtime() - t; //calculate time difference
	
	printf ("elapsed time = %f seconds\n",t);
	
	// free the dynamically allocated memory
	free(A);	
}


