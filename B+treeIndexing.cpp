#include <iostream>
#include <fstream>
#include <cmath>
#include <sstream>
#include <vector>
#include <string.h>
#include <stdlib.h>
#include <cstdio>
using namespace std;
void updateMetadata();
string datafile;
string indexfile;
int keylength;
int degree;
long int rootaddr;
int blocksize = 1024;
// defined a structure of type node with parameters
struct Node
{
	long address;
    bool leaf;
    vector<string> key;
    vector<long> child;
    vector<long> reference;
    long nextblock; 
    long prevblock; 
} node;
// defined a method to iniialise single valued node		  
void initSingleValuedNode(Node node, bool leaf, string key, long value, long pointer)
	{
		node.leaf = leaf;
        node.nextblock = -1;
        node.prevblock = -1;
        node.address = -1;

        node.key.push_back(key);

        if (leaf==true)
            node.reference.push_back(value);
        else
            node.child.push_back(pointer);
	}
// defined a method to iniialise multiple valued node	
void initMultivaluedNode(Node node, bool leaf, vector<string> key, vector<long> values, vector<long> pointer)
    {
        node.leaf = leaf;
        node.nextblock = -1;
        node.prevblock = -1;
        node.address = -1;
        int i=0;
        while (i<key.size())
        {
        	node.key.push_back(key[i]);
        	i++;
		}

        if (leaf==true)
        {
        	int i=0;
        	while (i<values.size())
        	{
        		node.reference.push_back(values[i]);
                i++;
			}
               	
		}
        else
        {   
		    int i=0;
           	while(i<pointer.size())
           	{
           		node.child.push_back(pointer[i]);
               	i++;
			}
               	
		}         
    }
// Declared and defined the Block copy function to copy data from source to destination  
void blockCopy(void *destination, void *source, size_t size) 
{ 
   char *csource = (char *)source; 
   char *cdestination = (char *)destination; 
   int i=0;
   while (i<size) 
   {
   	cdestination[i] = csource[i];
   	i++;
   }
         
}

// implemented the b-plus tree for indexing
void initBPlusTree()
{
//retrived parameters like data filename, keylength and degree from first block
    long offset = 0;
    char buffer[blocksize];

    ifstream inputfile;
    inputfile.open(indexfile.c_str(), ios::app | ios::in | ios::binary);
    inputfile.read(buffer, blocksize);
    inputfile.close();

// read datafile
    string datafilename(buffer, 257);
    int endindex = datafilename.find("0000"); // assumming no filename has 4 0s
    datafilename = datafilename.substr(0, endindex);
    offset = offset + 257;

// read keylength
    blockCopy(&keylength, buffer + offset, sizeof(keylength));
    offset = offset + sizeof(keylength);

// read degree
    blockCopy(&degree, buffer + offset, sizeof(degree));
    offset = offset + sizeof(degree);

// read location of root
    blockCopy(&rootaddr, buffer + offset, sizeof(rootaddr));
    offset = offset + sizeof(rootaddr);
}

// implemented method to remove Node from memory 
void deleteNode(Node node)
    {
        node.leaf = false;
        node.key.clear();
        node.child.clear();
        node.reference.clear();
        node.nextblock = -1;
        node.prevblock = -1;
        
    }
    
void readDisk(Node node)
    {
    	char *buffer = new char[blocksize];
        long offset = 0; 
        if (node.address <= 0) 
            return;


        ifstream inputfile;
        inputfile.open(indexfile.c_str(), ios::app | ios::in | ios::binary);
        inputfile.seekg(node.address);
        inputfile.read(buffer, blocksize);
        inputfile.close();

// read leaf 
        blockCopy(&node.leaf, buffer + offset, sizeof(bool));
        offset = offset + sizeof(node.leaf);

// read next address 
        blockCopy(&node.nextblock, buffer + offset, sizeof(long));
        offset = offset + sizeof(long);

// read prev address 
        blockCopy(&node.prevblock, buffer + offset, sizeof(long));
        offset = offset + sizeof(long);

// read the number of keys 
        long keysize;
        blockCopy(&keysize, buffer + offset, sizeof(keysize));
        offset = offset + sizeof(keysize);

// read keys
        node.key.clear();
        int i = 0;
        while (i<keysize) 
        {
            string keys(buffer + offset, keylength); 
            offset = offset + keylength + 1; 
            node.key.push_back(keys);
            i++;
        }

        if (node.leaf == true) 
        {
        	node.reference.clear();
        	long j=0;
            while(j < keysize)  
            {
                long pointer;
                blockCopy(&pointer, buffer + offset, sizeof(pointer));
                offset = offset + sizeof(pointer);
                node.reference.push_back(pointer);
                j++;
            }
            
        } 
        else
        {
            node.child.clear();
            int i = 0;
            while (i < keysize)
            {
                long child;
                blockCopy(&child, buffer + offset, sizeof(child));
                offset = offset + sizeof(child);
                node.child.push_back(child);
                i++;
            }
        }
    }
  
  
void readFromAddress(Node node, long address)
    {
        node.address = address;
        readDisk(node);
    }
    
 Node* getChild(Node node,int index)
    {
    	Node n;
        long address = node.child[index];
        readFromAddress(n, address);
        Node *p=&n;
        return p;
    }  
    
Node* splitIndexNode(Node* indexnode, string &parentkey) 
{
    // splitting internal node - has (2*degree + 1) keys and (2*degree + 2) pointers
    parentkey = indexnode->key[degree];
    indexnode->key.erase(indexnode->key.begin() + degree);
    
    // keep first degree keys and degree+1 pointers
    // move degree keys and degree+1 pointers to new node
    vector<string> newkey;
    vector<long> newchild;

    newchild.push_back(indexnode->child[degree + 1]);
    indexnode->child.erase(indexnode->child.begin() + degree + 1);
    
    while (indexnode->key.size() > degree) 
    {
        newkey.push_back(indexnode->key[degree]);
        indexnode->key.erase(indexnode->key.begin() + degree);
        newchild.push_back(indexnode->child[degree + 1]);
        indexnode->child.erase(indexnode->child.begin() + degree + 1);
    }

    vector<long> vector1;
    Node newnode;
	initMultivaluedNode(newnode, false, newkey, vector1, newchild);
    Node *p = &newnode;
    return p;
}

Node* splitleafnode(Node* leafnode) 
{
    vector<string> newkey;
    vector<long> newreference;
    
    // move 'degree' entries to the new node
    int i=degree;
    while(i <= 2*degree)
    {
        newkey.push_back(leafnode->key[i]);
        newreference.push_back(leafnode->reference[i]);
        i++;
    }
    
    // keep first 'degree' entries in the original leaf node
    int j=degree;
    while(j <= 2*degree)
    {
        leafnode->key.pop_back();
        leafnode->reference.pop_back();
        j++;
    }
    
    vector<long> vector1;
    Node newnode;
	initMultivaluedNode(newnode, true, newkey, newreference, vector1);
    Node *p = &newnode;
    return p;
}  

void writingIntoDisk(Node node)
    {
    	char bufferarray[1024];
        long offset = 0;
        
    	if (node.address == -1)
        {
            // get end of file offset
            ifstream inputfile;
            inputfile.open(indexfile.c_str(), ios::app | ios::in | ios::binary);
            inputfile.seekg(0, ios::end);
            node.address = inputfile.tellg();
            inputfile.close();
        }

        // write leaf bool
        blockCopy(bufferarray + offset, &node.leaf, sizeof(node.leaf));
        offset =  offset + sizeof(node.leaf);

           // write next pointer
        blockCopy(bufferarray + offset, &node.nextblock, sizeof(node.nextblock));
        offset = offset + sizeof(node.nextblock);

        // write prev pointer
        blockCopy(bufferarray + offset, &node.prevblock, sizeof(node.prevblock));
        offset = offset + sizeof(node.prevblock);
        
        // write number of keys in this node
        long keysize = node.key.size();
        blockCopy(bufferarray + offset, &keysize, sizeof(keysize));
        offset = offset + sizeof(keysize);

        // write keys
        
        for (vector<string>::const_iterator st=node.key.begin(); st!= node.key.end(); ++st)
        {
            blockCopy(bufferarray + offset, static_cast<void*>(&st), (*st).size()+1);
			offset =offset + (*st).size()+1;
        }

        // Add the child pointers to memory
        if (node.leaf == false) // internal node - write long children 
        {
        	
        	for (std::vector<long>::const_iterator ch=node.child.begin(); ch != node.child.end(); ++ch)
            {
                blockCopy(bufferarray + offset, &ch, sizeof(ch));
                offset = offset +  sizeof(ch);
            }
        } 
        else  // leaf node - write long pointers
        {
        	for (std::vector<long>::const_iterator pt=node.reference.begin(); pt != node.reference.end(); ++pt) 
            {
                blockCopy(bufferarray + offset, &pt, sizeof(pt));
                offset = offset + sizeof(pt);
            }
        }

        ofstream outputfile;
        outputfile.open(indexfile.c_str(), ios::app | ios::out | ios::binary | ios::in);
        outputfile.seekp(node.address, ios::beg);
        outputfile.write(bufferarray, blocksize);
        outputfile.close();

        deleteNode(node);
    }

Node* splitLeafNode(Node* leaf) 
{
    vector<string> nkeys;
    vector<long> nreferences;
    
    // move 'degree' entries to the new node
    for(int i = degree ; i <= 2*degree ; i++) 
    {
        nkeys.push_back(leaf->key[i]);
        nreferences.push_back(leaf->reference[i]);
    }
    
    // keep first 'degree' entries in the original leaf node
    for(int i = degree ; i <= 2*degree ; i++) 
    {
        leaf->key.pop_back();
        leaf->reference.pop_back();
    }
    
    vector<long> vector1;
    Node* newnode;
	initMultivaluedNode(*newnode, true, nkeys, nreferences, vector1);
    
    return newnode;
}

Node* insertRecordInBtree(Node* root, string keys, long offset)
{
    readDisk(*root); // bring root into the memory buffer
    if(!root->leaf) // root is internal node
    {
        // find the position of the first key which is greater than key to insert
        Node* index = root;
        int positionkey = 0;
        while (positionkey < index->key.size()) 
        {
            if (keys.compare(index->key[positionkey]) < 0) 
                break;
            positionkey++;
        }

        // insert this entry recursively in the ith child pointer of this internal node
        Node* nchild = insertRecordInBtree(getChild(*index,positionkey), keys, offset);
        
        if(nchild == NULL) // no splitting occurred in this node's child
        {
            return NULL;
        } 
        else // splitting occurred, now add a new pointer to this internal node
        {
            // find the first corresponding pointer whose key is greater than newchild_key
            int indexkey = 0;
            string nchildkey = nchild->key[0];
            while (indexkey < index->key.size()) 
            {
                if(nchildkey.compare(index->key[indexkey]) < 0) 
                    break;
                indexkey++;
            }

            // check if we have to 
            if (indexkey >= index->key.size()) 
            {
                index->key.push_back(nchildkey);
                index->child.push_back(nchild->address);
            }
            else 
            {
                index->key.insert(index->key.begin() + indexkey, nchildkey);
                index->child.insert(index->child.begin() + indexkey + 1, nchild->address);
            }

            // insert the new pointer in this node as it has space remaining
            if (index->key.size() <= 2 * degree)
            {
                writingIntoDisk(*index); // write out to file and delete from buffer
                return NULL;
            }
            else // split this node because it's full
            {
                // original node is index and new node is new_child
                string parentkey = "";
                nchild = splitIndexNode(index, parentkey);

                // root was just split
                if (index->address == rootaddr) 
                {
                    // create a new node new_root containing index and newchild nodes as pointers
                    // and make the root bptree's pointer point to new_root
                    writingIntoDisk(*index);
                    writingIntoDisk(*nchild);

                    // add the index and newchild as children of the new_root
                    vector<long> newchild;
                    newchild.push_back(index->address);
                    newchild.push_back(nchild->address);

                    vector<long> vector1; // empty variable that will be ignored in the constructor
                    vector<string> nkeys;
                    nkeys.push_back(parentkey);

                    // create the new_root
                    Node* newroot;
					initMultivaluedNode(*newroot, false, nkeys, vector1, newchild);
					
                    writingIntoDisk(*newroot);

                    // update the root_address and update the first metadata block using update_metadata()
                    rootaddr = newroot->address;
                    updateMetadata();

                    return NULL;
                }
                return nchild;
            }
        }
    }
    else // root is leaf node
    {
        Node* leaf = root;
        
        // find the position of the first key which is greater than key to insert
        if (keys.compare(leaf->key[0]) < 0) // insert at the beginning
        {
            leaf->key.insert(leaf->key.begin(), keys);
            leaf->reference.insert(leaf->reference.begin(), offset);
        } 
        else if (keys.compare(leaf->key[leaf->key.size() - 1]) > 0) // insert at the end
        {
            leaf->key.push_back(keys);
            leaf->reference.push_back(offset);
        } 
        else // insert somewhere in between
        {
            for(int indexkey = 0 ; indexkey < leaf->key.size() ; indexkey++)
            {
                if (leaf->key[indexkey].compare(keys) > 0) {
                    leaf->key.insert(leaf->key.begin() + indexkey, keys);
                    leaf->reference.insert(leaf->reference.begin() + indexkey, offset);
                    break;
                }
            }
        }

        // since this leaf has space, insert this entry recursively in the ith position
        if(leaf->key.size() <= 2 * degree) 
        {
            writingIntoDisk(*leaf);
            return NULL;
        }
        else // splitting occurred, now add a new pointer to this leaf node
        {
            Node* nchild = splitLeafNode(leaf);

            if (leaf->address == rootaddr) // if this leaf was the root, make a new root
            {
                vector<string> nkeys;
                nkeys.push_back(nchild->key[0]);

                // set prev/next siblings - point prev's next and next's prev to newchild
                long temp = leaf->nextblock;
                nchild->prevblock = leaf->address;
                nchild->nextblock = temp;
                if (temp != -1) 
                {
                    Node *n;
                    readFromAddress (*n,temp);
                    n->prevblock = nchild->address; // check against NULL next pointer
                    writingIntoDisk(*n);
                }

                writingIntoDisk(*nchild);
                leaf->nextblock = nchild->address;
                writingIntoDisk(*leaf);

                // add leaf and newchild pointers to our new_root
                vector<long> newchild;
                newchild.push_back(leaf->address);
                newchild.push_back(nchild->address);
                vector<long> vector1;

                // create the new_root
                Node* newroot;
                initMultivaluedNode(*newroot, false, nkeys, vector1, newchild);
                newroot->leaf = false;
                writingIntoDisk(*newroot);

                // update the root_address and update the first metadata block using update_metadata()
                rootaddr = newroot->address;
                updateMetadata();

                return NULL;
            }
            else
            {
                writingIntoDisk(*nchild);
                readDisk(*nchild); // write and read to get a valid address for newchild

                // set prev/next siblings - point prev's next and next's prev to newchild
                long temp = leaf->nextblock;
                leaf->nextblock = nchild->address;
                nchild->prevblock = leaf->address;
                nchild->nextblock = temp;
                if (temp != -1) 
                {
                    Node *n; 
					readFromAddress(*n,temp);
                    n->prevblock = nchild->address; // check against NULL next pointer
                    writingIntoDisk(*n);
                }

                writingIntoDisk(*nchild);
                writingIntoDisk(*leaf);
            }
            return nchild;
        }
    }
}
 
      

    
    
long findRecord(Node* root, string keys)
{
    if (root == NULL)
        return 0;

    readDisk(*root);
    if (root->leaf) // reached a leaf node
    {
        // iterate through all values of this node
        int keyindex = 0;
        while(keyindex < root->key.size())
        {
            if (keys.compare(root->key[keyindex]) == 0) // if any key matches exactly, return it
            {
                long p = root->reference[keyindex];
                deleteNode(*root);
                return p;
            }
            keyindex++;
        }
        return -1; // return -1 if no key matches
    }
    else // route the search query in internal nodes after comparing key
    {
        // if search key is smaller than ith key of internal node
        int keyindex = 0;
        while(keyindex < root->key.size())
        {
            // go down the child pointer whose corresponding key is less than the target
            if (keys.compare(root->key[keyindex]) < 0)
            {
                Node* c = getChild(*root, keyindex);
                deleteNode(*root);
                return findRecord(c, keys);
            }
        }
        Node* c = getChild(*root, root->child.size() - 1);
        deleteNode(*root);
        return findRecord(c, keys);  // follow rightmost pointer
    }
}  

void printRecordAtOffset(long offsetkey)
{
    // open file at offset address, read till end of line
    char buffer[1001] = "";
    ifstream inputfile(datafile);
    inputfile.seekg(offsetkey, inputfile.beg);
    inputfile.read(buffer, 1000);
    inputfile.close();
    string str(buffer);
    cout << str.substr(0, str.find("\n")) << endl;
}  

void listRecordCount(Node* root, string keytarget, int count)
{
    readDisk(*root); // bring the current node into the buffer
    if (root->leaf) // if its a leaf
    {
    	int i = 0;
		while(i < root->key.size())
        {
            // if any key matches or our target is between any 2 consecutive keys or there's only 1 key
            if (keytarget.compare(root->key[i]) == 0 || 
                (i > 0 && keytarget.compare(root->key[i-1]) < 0 && keytarget.compare(root->key[i]) > 0) || 
                (root->key.size() == 1))
            {
                // start printing from here till 'count' next nodes
                while (root && count > 0)
                {
                    for(int a = i ; a < root->key.size() && count > 0 ; a++)
                    {
                        cout << "[" << root->reference[a] << "]: ";
                        count--;
                        printRecordAtOffset(root->reference[a]);
                    }
                    cout << endl;

                    if (root->nextblock == -1) break; // follow the next pointer to a sibling leaf node
                    long nextroot = root->nextblock;
                    deleteNode(*root);
                    readFromAddress(*root,nextroot);
                    
                }
                return;
            }
            i++; 
        }
    }
    else
    {
        // if search key is smaller than ith key of internal node
        int indexkey = 0;
        while(indexkey < root->key.size())
        {
            if (keytarget.compare(root->key[indexkey]) < 0) // go down the child pointer whose corresponding key is smaller
            {
                Node* c = getChild(*root,indexkey);
                deleteNode(*root);
                return listRecordCount(c, keytarget, count);
            }
            indexkey++;
        }

        // didn't match any - going for last child pointer
        Node* c = getChild(*root,root->child.size() - 1);
        deleteNode(*root);
        return listRecordCount(c, keytarget, count); // follow rightmost pointer
    }
}
void createIndex(string datafilename, string indexfilename, int keylen, long newrootaddress, bool update=false)
{
    /* create index file with one 1024kb block
     * structure:
     * data filename (256 bytes)
     * key length (4 bytes - int)
     * degree (4 bytes - int)
     * root_address (8 bytes - long)
     */
    long offset = 0;
    char buffer[blocksize];

    // write filename in first 256 bytes
    string filename = datafilename.append(string((256 - datafilename.length()), '0'));
    blockCopy(buffer + offset, static_cast<void*>(&filename), strlen(filename.c_str()) + 1);
    //blockCopy(buffer + offset, static_cast<void*>(filename), (*st).size()+1);
    offset = offset + strlen(filename.c_str()) + 1;

    // write keylen
    blockCopy(buffer + offset, &keylen, sizeof(keylen));
    offset = offset + sizeof(keylen);

    // calculate degree of a node (a node can store degree <= n <= 2*degree key-value pairs)
    // assume 50 bytes for metadata (on the safe side)
    // the exact number of bytes used in a block apart from records = 25 bytes (3 longs and a bool)
    int degree = (blocksize - 50)/ ((keylen+8)*2); // each record is key_length bytes + 8 bytes for a long

    // write degree
    blockCopy(buffer + offset, &degree, sizeof(degree));
    offset = offset + sizeof(degree);

    // write root address with default as 1024 otherwise use the global variable value
    if (newrootaddress == -1) 
    {
        newrootaddress = 1024;
        rootaddr = 1024;
    }
    blockCopy(buffer + offset, &newrootaddress, sizeof(newrootaddress));
    offset = offset + sizeof(newrootaddress);

    // copy buffer to file
    ofstream outputfile;
    // we open the output file as ios::in and ios::out when we're updating it
    // but open only as ios::out when we're creating it for the first time
    if (update) 
        outputfile.open(indexfilename.c_str(), ios::app | ios::in | ios::out | ios::binary);
    else
        outputfile.open(indexfilename.c_str(), ios::app | ios::out | ios::binary);
    outputfile.write(buffer, blocksize);
    outputfile.close();

    if (update) // if this was just an update then no need to insert everything again
        return;

    // initialize the root of the bplus tree
    indexfile = indexfilename;
    initBPlusTree();

    // iterate through the data file and keep inserting records into index
    ifstream inputfile(datafile);
    string line;
    offset = 0;

    int count = 0;
    Node* root;
    bool firsttime = true;

    // for each record
    while (getline(inputfile, line)) 
    {
        // insert pair(string, offset) in records array and update offset
        string key = line.substr(0, keylength);
        if (firsttime) // for the first insert, create a root otherwise read the root_address
        {
            
            initSingleValuedNode(*root, true, key, offset, NULL);
            firsttime = false;
        }
        else
        {
            readFromAddress(*root,rootaddr);
        }

        insertRecordInBtree(root, key, offset);
        offset = inputfile.tellg();
        count++;
    }
    cout << "Successfully inserted " << count << " records in index file b+ tree." << endl;
}

void updateMetadata()
{
    createIndex(datafile, indexfile, keylength, rootaddr, true);
}

void listRecords(string indexfilename, string keytarget, int count)
{
    indexfile = indexfilename;
    initBPlusTree();
    Node* root;
    readFromAddress(*root,rootaddr);
     

    listRecordCount(root, keytarget, count);
}



int main(int argc, char** argv) 
{
	if (argc > 6 || argc < 4  )
    {
        cout << "Provide correct number of parameters";
        return 0;
    }
    string choice=argv[1];
    char c=choice.at(0);
    
    switch(c)
    {
    	
    	case 'c' :
    	{
    		string datafile(argv[2]);
            string indexfile(argv[3]);
            int keylength = atoi(argv[4]);
           // createindex(datafile, indexfile, keylength, -1, false);
            break;
        }
        case 'l' :
        {	
           string indexfile(argv[2]);
           string targetkey(argv[3]);
           int count = atoi(argv[4]);
           //listTheRecord(indexfile, key, count);
           break;
       }
       case 'i' :
       	{
       		string indexfile(argv[2]);
            string targetkey(argv[3]);
           // insertTheRecord(indexfile, key);
            break;
		}
		
		case 'f' :
		{
			string index_file(argv[2]);
            string target_key(argv[3]);
           // findIndex(indexfile, tkey);
		}
        
    }
}
    
