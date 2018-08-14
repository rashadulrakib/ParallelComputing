/*
Author: Md Rashadul Hasan Rakib: B00598853
This is an in-place modified sequential program that selects the ith smallest value within an array.
The modification has been done to handle the duplicate items so as to keep the time complexity O(N)
Input: 
	An array of elements
	Index of the ith smallest element within the array
Output:
	select the ith smallest value
*/
#include <stdio.h>
#include<stdlib.h>
#include<omp.h>
#define MAXSIZE	2000000000 //2 billion items
#define MAX_VAL MAXSIZE //generate random numbers within 0 to MAX_VAL-1
#define ITH_ELEMENT MAXSIZE/2 //index of the median element

int randomizedSelect(int * A, int p, int r, int i);
int randomizedPartition(int * A,int p, int r);
int partition(int *A,int p, int r);
int travarseLeftDuplicates (int * A,int p,int r,int i, int pivot);
int travarseRightDuplicates(int * A,int p,int r,int i, int pivot);
void swap(int *i, int *j);

int main()
{
	int p=0; //initial left index
	int r=MAXSIZE-1; //initial right index
	int i=ITH_ELEMENT; //index of the ith element
	int m;
	int * A =(int *)calloc(MAXSIZE, sizeof(int)); //allocate the memory for total items
		
	// Seed the random number generator to get different results each time
	srand((unsigned)time(0));
			
	for(m=0;m<MAXSIZE;m++){
		A[m]=rand()%MAX_VAL; //populate the array with random numbers from 0 to MAX_VAL-1
	}		
	
	double t = omp_get_wtime( ); //start the timer	
	
	int ithElem = randomizedSelect(A,p,r,i); //call randomizedSelect to get the ith element
	printf("-------------------- %dth smallest value=%d ---------------\n",i, ithElem);
	
	t = omp_get_wtime() - t; //calculate time difference
	
	printf ("elapsed time = %f seconds\n",t);
	
	// free the dynamically allocated memory
	free(A);
}

//this function return the ith smallest value within the array A
int randomizedSelect(int * A, int p, int r, int i){
	//A: Array of the items
	//p: left index of the data block within which I find the ith smallest item
	//r: right index of the data block within which I find the ith smallest item
	//i: index of the ith smallest item: which can be considered as the distance from left position

	if(p==r){ //when one item within the block
		return A[p];
	}
	int q=randomizedPartition(A,p,r); //index of the pivot within p to r of A such that, left items <pivot : right items >= pivot
	int k=q-p+1; //distance of pivot from the left position within the block
	if(i==k){ //distance of the ith element = distance of pivot: pivot value is the answer
		return A[q];
	}else if(i<k){ //distance of the ith element < distance of pivot  
		int pivot = A[q];
		q = travarseLeftDuplicates(A,p,q-1,i,pivot); //traverse left direction to find the consecutive duplicates
		if(q==-1){ //q=-1 indicates that we find the ith element during the left traverse and we return the ith element 
			return pivot;
		}else{
			return randomizedSelect(A,p,q-1,i); //recursive call on the left part of A
		}
	}else{
		int pivot = A[q];
		q = travarseRightDuplicates(A,q+1,r,i-k,pivot); //traverse right direction to find the consecutive duplicates
		if(q==-1){ //q=-1 indicates that we find the ith element during the right traverse and we return the ith element
			return pivot;
		}else{
			return randomizedSelect(A,q+1,r,i-k); //recursive call on the right part of A
		}
	}
	return 0;
}

//this function traverse in the left direction within p to r index as long as it finds consecutive duplicate pivot values
int travarseLeftDuplicates(int * A,int p,int r,int i, int pivot){
	int m;
	int q=r+1; //keep the original partition position of the pivot element
	for(m=r;m>=p;m--){ //traversing in the left direction within the block
		if(A[m]==pivot && (m-p+1)>=i){ //As long as the pivot item is same as the item in the left direction 
										//and the distance I traverse (in left direction) 					
										//is greater than or equal to the distance of the ith element.
			q = m;	//update the index of the partition as long as we traverse 
			if((m-p+1)==i){ // distance of the ith element and mth element same
				return -1; // -1 indicates that we find the ith smallest item
			}
		}else{
			break; //the pivot is not same in the left direction or the distance I traverse 				
					//(in left direction) is less than the distance of the ith element.
		}
	}
	
	return q; //return the index of the item which is not same as pivot
}

//this function traverse in the right direction within p to r index as long as it finds consecutive duplicate pivot values
int travarseRightDuplicates(int * A,int p,int r,int i, int pivot){
	int m;
	int q=p-1; //keep the original partition position of the pivot element
	for(m=p;m<=r;m++){ //traversing in the right direction within the block
		if(A[m]==pivot && (m-p+1)<=i){ //As long as the pivot item is same as the item 				
										//in the right direction and distance I traverse (in right direction) is less 				
										//than or equal to the distance of the ith element.
			q = m; //update the index of the partition as long as we traverse
			if((m-p+1)==i){ // distance of the ith element and mth element same
				return -1; // -1 indicates that we find the ith smallest item
			}
		}else{
			break; //the pivot is not same in the right direction or the distance I traverse 				
					//(in right direction) is greater than the distance of the ith element.
		}
	}
	
	return q; //return the index of the item which is not same as pivot
}

// this function call the partition function for a random value
int randomizedPartition(int * A,int p, int r){
	int i = rand()%(r-p+1)+p; //select a random index within p to r
	swap(&A[r],&A[i]); //swap the ith value with last value in r
	return partition(A,p,r);
}

//this function returns the partition index of a random item within p to r
int partition(int * A,int p, int r){
	int x=A[r];
	int i=p-1;
	int j;
	for(j=p;j<=(r-1);j++){
		if(A[j]<=x){
			i=i+1;
			swap(&A[i],&A[j]);
		}
	}
	swap(&A[i+1],&A[r]);
	return i+1; //return the index
}

//this function is for swapping two values
void swap(int *i, int *j) {
   int t = *i;
   *i = *j;
   *j = t;
}
