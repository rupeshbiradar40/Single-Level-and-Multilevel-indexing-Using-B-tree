#include <iostream>
//#include <io.h>
#include <stdlib.h>
#include <fstream>
#include<vector>
#include <string.h>
#include <bits/stdc++.h> 

using namespace std;
//declared functions to create indexfile  and list datafile
void CreateIndexFile(string, string, int);
void ListDataFile(string, string, int);
int keylength;
int main(int argc, char *argv[])
{
// Validated the total number of parameters
	if(argc!=6){
		cout<<"Please provide all the parameters";
		return 0;
	}
//stored the contents of comand line parameters to string to use them further in the program
	string datafile=argv[3];
	string indexfile=argv[4];
	keylength=atoi(argv[5]);
// Validated the size of the key to be entered
	if (keylength<1 || keylength>24){
		cout<<"allowed keylength is between 1-24 only";
		return 0;
	}
// Implemented switch stateent to call the respective functions of user choice
	string choice=argv[2];
	if(strcmp(argv[2],"-l") != 0 && strcmp(argv[2],"-c") != 0) {
		cout<<"ERROR: First argument should be either \'-l\' or \'-c\'!\n";
		return 0;
	}
	if (choice.compare("-c")==0)
		CreateIndexFile(datafile,indexfile,keylength);
	if (choice.compare("-l")==0)
		ListDataFile(datafile,indexfile,keylength);
	return 0;
}
//Defined a structure to store index entry and offset
struct Index{
	char key[24];	
	long int offset;
};
//wrote a function to convert binary offset into string
string convertBinaryToString(char* s1, int size) 
{ 
    int j; 
    string st = ""; 
    for (j = 0; j < size; j++) { 
        st = st + s1[j]; 
    } 
    return s1; 
} 
// wrote a function to sort the vector using the index keys stored in it and called it in sort function
bool sortby(Index in1,Index in2) 
{ 
	string s1 = convertBinaryToString(in1.key, keylength);
	string s2 = convertBinaryToString(in2.key, keylength);
	return (s1 < s2);
}



// this function is executed when user provides -C parameter and create index file
void CreateIndexFile(string datafile, string indexfile, int keylength)
{
	fstream file;
	string record;
	int i=0;
	file.open(datafile, ios::in);
	vector<Index>entry;
	
//Implemented logic to add first record in index file
	getline(file,record);
	Index in;
	for(i=0;i<keylength;i++)
		in.key[i]=record.at(i);
	    in.key[i]='\0';
	    in.offset=0;
	entry.push_back(in);
	
//Implemented logic to add subsequent records in index file
	int t=file.tellg();
	while (getline(file,record))
	{
		for(i=0;i<keylength;i++)
		 in.key[i]=record.at(i);
		in.key[i]='\0';
		in.offset=t-1;
		t=file.tellg();
		entry.push_back(in);
	}
// Called the sort function to sort the vector
	std::sort (entry.begin(), entry.end(), sortby);

	fstream fileout;
	fileout.open(indexfile, ios::out | ios::binary);
	for (auto it = entry.begin(); it != entry.end(); ++it)
	{
		struct Index in2;
		for(i=0;i<keylength;i++)
			in2.key[i]=it->key[i];
		in2.key[i]='\0';
		in2.offset=it->offset;
		fileout.write(reinterpret_cast<char *>(&in2),sizeof(Index));	
		fileout<<endl;
	}	
}
// this function is executed when user provides -l parameter and list datafile using indexfile
void ListDataFile(string datafile, string indexfile, int keylength)
{
	fstream filecount,fileindex,filedata;
	string recoed,val;
	string tuple;
	int count=0;
//Implemented the logic to count number of records in index file
	filecount.open(indexfile, ios::in | ios::binary); 
	while(getline(filecount,val))
		count++;
	filecount.close();
	fileindex.open(indexfile, ios::in | ios::binary);
	filedata.open(datafile, ios::in);
	Index in;
	fileindex.read(reinterpret_cast<char *>(&in), sizeof(Index));
	if(in.offset==0)
		filedata.seekg(in.offset);
	else
		filedata.seekg(in.offset+1);
	getline(filedata,tuple);
	cout<<tuple<<endl;
	while ( count-1)
	{ 
		getline(fileindex,val);
		tuple="";
		Index in;
		fileindex.read(reinterpret_cast<char *>(&in), sizeof(Index));
		if(in.offset==0)
			filedata.seekg(in.offset);
		else
			filedata.seekg(in.offset+1);
		getline(filedata,tuple);
		cout<<tuple<<endl;
		--count;
	}
}



































