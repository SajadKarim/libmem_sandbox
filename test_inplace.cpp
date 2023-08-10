// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2014-2017, Intel Corporation */

/*
 * simple_copy.c -- show how to use pmem_memcpy_persist()
 *
 * usage: simple_copy src-file dst-file
 *
 * Reads 4k from src-file and writes it to dst-file.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#endif
#include <string.h>
#include <libpmem.h>
#include <stdbool.h>
#include  <cassert>
#include <cstdint>
#include <math.h>
#include <chrono>
#include <thread>

bool createMMapFile(void*& hMemory, const char* szPath, size_t nFileSize, size_t& nMappedLen, int& bIsPMem)
{
        if ((hMemory = pmem_map_file(szPath,
                                nFileSize,
                                PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                                0666, &nMappedLen, &bIsPMem)) == NULL) 
	{
                perror("Failed to create a memory-mapped file.");
                return false;
        }

	return true;
}

bool openMMapFile(void*& hMemory, const char* szPath, size_t& nMappedLen, int& bIsPMem)
{
        if ((hMemory = pmem_map_file(szPath,
                                0,
                                0,
                                0666, &nMappedLen, &bIsPMem)) == NULL)
        {
                perror("Failed to open the memory-mapped file.");
                return false;
        }

        return true;
}

bool writeMMapFile(void* hMemory, const char* szBuf, size_t nLen)
{
	void* hDestBuf = pmem_memcpy_persist(hMemory, szBuf, nLen);

	if( hDestBuf == NULL)
	{
		perror("Failed to write to the memory-mapped file.");
		return false;
	}

	return true;
}

bool readMMapFile(const void* hMemory, char* szBuf, size_t nLen)
{
	//memcpy(szBuf, hMemory, nLen);
	//return true;
        void* hDestBuf = pmem_memcpy(szBuf, hMemory, nLen, PMEM_F_MEM_NOFLUSH);

        if( hDestBuf == NULL)
	{
		perror("Failed to read from the memory-mapped file.");
                return false;
	}

        return true;
}

#include <iostream>     // std::cout
#include <algorithm>    // std::shuffle
#include <array>        // std::array
#include <random>       // std::default_random_engine
#include <chrono>

void closeMMapFile(void* hMemory, size_t nMappedLen)
{
	pmem_unmap(hMemory, nMappedLen);
}

const int nTotalNodes = 1024;
const int nValueSizeInNode = 4096;
const int nTotalValuesInNode = 1024;

struct Node {
	char szValues[nTotalValuesInNode][nValueSizeInNode];
};

struct NodeA {
	char** szData;
};

std::array<int,1024> foo;

void test_direct_read_dram()
{
        int nIsPMem;
        size_t nMappedLen;
        //void* hMemory = NULL;
        char* szReadBuf = NULL;
        char* szWriteBuf = NULL;
        uint64_t nBytesInGB = 1024*1024*1024;
        uint64_t nFileSize = 10*nBytesInGB;
        uint64_t nBytesToWrite = 5*nBytesInGB;
        char* szFilePath = "/home/wuensche/pool/pool0";
/*
        if( !openMMapFile(hMemory, szFilePath, nMappedLen, nIsPMem))
        {
                if( !createMMapFile(hMemory, szFilePath, nFileSize, nMappedLen, nIsPMem))
                {
                        exit(1);
                }
        }
*/
	void* hMemory = operator new(nTotalNodes*sizeof(Node));

// std::cout << "shuffled elements:";
//  for (int& x: foo) std::cout << ' ' << x;
//  std::cout << '\n';

//        return;



        /*for( int nValueSize=2; nValueSize<pow(2, 13); nValueSize=nValueSize<<1)
        {
                printf("Executing for ValueSize:%d\n", nValueSize);

                char** szData = new char*[nTotalValuesInNode];
                for( int nIndex = 0, ch=65; nIndex<nTotalValuesInNode; nIndex++, ch++)
                {
                        szData[nIndex] = new char[nValueSize];
                        memset(&szData[nIndex], ch, nValueSize-1);
                        szData[nIndex][nValueSize-1]='\0';
                        ch = ch > 90 ? 65 : ch;

                        printf("...ch %d\n", ch);
                }
        }

        return;
        */

        uint64_t nOffset = 0;

        printf("Start writing the nodes. (NodeId,Offset):\n");
        for( int n=0; n<nTotalNodes; n++) {

                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++) {
                        memset(&oNode.szValues[i], ch, nValueSizeInNode-1);
                        oNode.szValues[i][nValueSizeInNode-1]='\0';
                        if( ch > 90)
                                ch = 65;
                }

                nOffset = ((uint64_t)n)*sizeof(Node);

                //printf("(%d,%llu),", n, nOffset);

                void* hDestBuf = memcpy(hMemory + nOffset, &oNode, sizeof(Node));

                if( hDestBuf == NULL)
                {
                        perror("Failed to write to the memory-mapped file.");
                        return;
                }
        }

        printf("\nStart reading the nodes.\n");
        for( int x=nValueSizeInNode; x>=2; x=x/2)
        {
		//std::this_thread::sleep_for (std::chrono::seconds(10));

                //printf("...Setting NULL character at the end of %d\n", x-1);

                for( int n=0; n<nTotalNodes; n++)
                {
                        nOffset = ((uint64_t)n)*sizeof(Node);
                        Node* oNode = reinterpret_cast<Node*>(hMemory + nOffset);

                        //printf("...reading to offset %llu\n", nOffset);

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                oNode->szValues[i][x-1] = '\0';
                        }
                }

                char* szTempBuffer = new char[x+1];
                memset(szTempBuffer, 0, x);
                szTempBuffer[x] = '\0';

               uint64_t sum = 0;
                auto tStart = std::chrono::high_resolution_clock::now();
                //auto tEnd = std::chrono::high_resolution_clock::now();
                //auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                for( int n=0; n<nTotalNodes; n++) // sequential
                //for( int n=nTotalNodes-1; n>=0; n--) // random
                {
                        // sequential
                        nOffset = ((uint64_t)n)*sizeof(Node);
                        Node* oNode = reinterpret_cast<Node*>(hMemory + nOffset);

                        for( int i=0; i<nTotalValuesInNode; i++) // sequential
                        //for( int i=nTotalValuesInNode-1; i>=0; i--) // random
                        {

                                // random
                                //nOffset = ((uint64_t)foo[i])*sizeof(Node);
                                //Node* oNode = reinterpret_cast<Node*>(hMemory + nOffset);

                                //tStart= std::chrono::high_resolution_clock::now();
//sum += x;
                                                                        // change i with n for random
                                memcpy(szTempBuffer, &oNode->szValues[i], x);
                                //printf("Read %d bytes for Node %d Index %d\n", x, nTotalValuesInNode, x);
                                //printf("::reading..:: Node:%d, ValueIndex:%d, Value:%s\n", n+1, i, szTempBuffer/*oNode->szValues[i]*/);
                                //
                                //tEnd = std::chrono::high_resolution_clock::now();
                                //tDiff += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                //auto tDiff2 = std::chrono::duration<uint64_t, std::nano>(tEnd2-tStart2).count();

                //printf("Total Data Read: %llu, Read Block Size: %d, Total time diff: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff2);

                         }
                        //break;
                }

                auto tEnd = std::chrono::high_resolution_clock::now();
                auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                printf("Total Data Read: %llu, Read Block Size: %d, Total time diff: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);

                //return;
        }

}




void test_direct_read()
{
	int nIsPMem;
        size_t nMappedLen;
        void* hMemory = NULL;
        char* szReadBuf = NULL;
        char* szWriteBuf = NULL;
        uint64_t nBytesInGB = 1024*1024*1024;
        uint64_t nFileSize = 10*nBytesInGB;
        uint64_t nBytesToWrite = 5*nBytesInGB;
        char* szFilePath = "/home/wuensche/pool/pool0";

        if( !openMMapFile(hMemory, szFilePath, nMappedLen, nIsPMem))
        {
        	if( !createMMapFile(hMemory, szFilePath, nFileSize, nMappedLen, nIsPMem))
                {
                	exit(1);
                }
        }


// std::cout << "shuffled elements:";
//  for (int& x: foo) std::cout << ' ' << x;
//  std::cout << '\n';

//	  return;



	/*for( int nValueSize=2; nValueSize<pow(2, 13); nValueSize=nValueSize<<1)
	{
		printf("Executing for ValueSize:%d\n", nValueSize);

		char** szData = new char*[nTotalValuesInNode];
		for( int nIndex = 0, ch=65; nIndex<nTotalValuesInNode; nIndex++, ch++)
		{
			szData[nIndex] = new char[nValueSize];
			memset(&szData[nIndex], ch, nValueSize-1);
			szData[nIndex][nValueSize-1]='\0';
			ch = ch > 90 ? 65 : ch;

			printf("...ch %d\n", ch);
		}
	}

	return;
	*/

	uint64_t nOffset = 0;

	printf("Start writing the nodes. (NodeId,Offset):\n");
	for( int n=0; n<nTotalNodes; n++) {
		
		Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++) {
                	memset(&oNode.szValues[i], ch, nValueSizeInNode-1);
			oNode.szValues[i][nValueSizeInNode-1]='\0';
                        if( ch > 90)
                        	ch = 65;
                }

		nOffset = ((uint64_t)n)*sizeof(Node);

                //printf("(%d,%llu),", n, nOffset);

                void* hDestBuf = pmem_memcpy_persist(hMemory + nOffset, &oNode, sizeof(Node));

                if( hDestBuf == NULL)
                {
			perror("Failed to write to the memory-mapped file.");
                        return;
                }
        }

	printf("\nStart reading the nodes.\n");
	for( int x=nValueSizeInNode; x>=2; x=x/2) 
	{
		//std::this_thread::sleep_for (std::chrono::seconds(10));

		//printf("...Setting NULL character at the end of %d\n", x-1);

		for( int n=0; n<nTotalNodes; n++)
                {
			nOffset = ((uint64_t)n)*sizeof(Node);
                        Node* oNode = reinterpret_cast<Node*>(hMemory + nOffset);

			//printf("...reading to offset %llu\n", nOffset);

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                oNode->szValues[i][x-1] = '\0';
			}
                }

		char* szTempBuffer = new char[x+1];
		memset(szTempBuffer, 0, x);
		szTempBuffer[x] = '\0';
		uint64_t sum = 0;		
		auto tStart = std::chrono::high_resolution_clock::now();
		//auto tEnd = std::chrono::high_resolution_clock::now();
                //auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

       		for( int n=0; n<nTotalNodes; n++) // sequential
		//for( int n=nTotalNodes-1; n>=0; n--) // random
		{
			// sequential
			nOffset = ((uint64_t)n)*sizeof(Node);
			Node* oNode = reinterpret_cast<Node*>(hMemory + nOffset);
			
			for( int i=0; i<nTotalValuesInNode; i++) // sequential
			//for( int i=nTotalValuesInNode-1; i>=0; i--) // random
			{

				// random
				//nOffset = ((uint64_t)foo[i])*sizeof(Node);
	                        //Node* oNode = reinterpret_cast<Node*>(hMemory + nOffset);

				//tStart= std::chrono::high_resolution_clock::now();
//sum += x;
									// change i with n for random
				pmem_memcpy(szTempBuffer, &oNode->szValues[i], x, PMEM_F_MEM_NOFLUSH);
				//printf("Read %d bytes for Node %d Index %d\n", x, nTotalValuesInNode, x);
				//printf("::reading..:: Node:%d, ValueIndex:%d, Value:%s\n", n+1, i, szTempBuffer/*oNode->szValues[i]*/);
				//
				//tEnd = std::chrono::high_resolution_clock::now();
				//tDiff += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                //auto tDiff2 = std::chrono::duration<uint64_t, std::nano>(tEnd2-tStart2).count();

                //printf("Total Data Read: %llu, Read Block Size: %d, Total time diff: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff2);

                	 }
			//break;
         	}

	        auto tEnd = std::chrono::high_resolution_clock::now();
        	auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

		printf("Total Data Read: %llu, Read Block Size: %d, Total time diff: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);

		//return;
	}
	
}

bool test_copying();

int main(int argc, char *argv[])
{

//	  std::array<int,1024> foo;
        for( int idx = 0; idx < 1024; idx++) {
                foo[idx] = idx;
        }

        // obtain a time-based seed:
        unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();

        shuffle (foo.begin(), foo.end(), std::default_random_engine(seed));

	//printf("size of node %llu\n", sizeof(Node));
	test_direct_read_dram();
	std::this_thread::sleep_for (std::chrono::seconds(10));
	test_direct_read();
	std::this_thread::sleep_for (std::chrono::seconds(10));
	test_copying();
	
	printf("size of node %llu\n", sizeof(Node));
}

bool test_copying()
{
	int nIsPMem;
	size_t nMappedLen;
	void* hMemory = NULL;
	char* szReadBuf = NULL;
	char* szWriteBuf = NULL;
	uint64_t nBytesInGB = 1024*1024*1024;
	uint64_t nFileSize = 10*nBytesInGB;
	uint64_t nBytesToWrite = 5*nBytesInGB;
	char* szFilePath = "/home/wuensche/pool/pool0";

	uint64_t nWriteTime = 0;
	uint64_t nReadTime = 0;

	size_t nBufSize=4; 

	unsigned long long nodes[1024];

	uint64_t nOffset = 0;

	//for(size_t nBufSize=4; nBufSize<pow(2,20); nBufSize=nBufSize<<1)
	{
		//printf("Running for buffer size = %d.\n", nBufSize);

		szWriteBuf = new char[nBufSize];
		for(size_t nIdx=0; nIdx<nBufSize; nIdx++)
		{
        		szWriteBuf[nIdx] = (rand() % 10);
	     	}	

		if( !openMMapFile(hMemory, szFilePath, nMappedLen, nIsPMem))
		{
			if( !createMMapFile(hMemory, szFilePath, nFileSize, nMappedLen, nIsPMem))
			{
				exit(1);
			}
		}

		/*if (!nIsPMem) 
		{
			perror("Not a persistent memory module.");
			exit(1);
		}*/

		//printf("Started writing the blocks. %d\n", nMappedLen);
		//

		printf("Start writing the nodes. (NodeId,Offset):\n");
		
		char* szBuffer = new char[sizeof(Node)];
		for( int n=0; n<nTotalNodes; n++) 
		{
                	Node oNode;
			for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++) 
			{
				memset(&oNode.szValues[i], ch, nValueSizeInNode-1);
				oNode.szValues[i][nValueSizeInNode-1]='\0';
				if( ch > 90) 
					ch = 65;
			}

			nOffset = ((uint64_t)n)*sizeof(Node);

			//printf("(%d,%llu),", n, nOffset);
			
			memcpy(szBuffer, &oNode, sizeof(Node));
			
			if(!writeMMapFile(hMemory + (n+1)*sizeof(Node), szBuffer, sizeof(Node)))
				return -1;
		}



		/*char* szBuffer = new char[sizeof(Node)];
		int nNodeSize = 0;
		for( int n=0, t=sizeof(nodes)/sizeof(unsigned long long); n<t; n++) {
			printf("..::writing:: working on node %d\n", n+1);

			Node pNodeA;
			for( int i=0, ch=65; i<1024; i++, ch++) {
				memset(&pNodeA.szValues[i], ch, 1023);
				pNodeA.szValues[i][1023]= '\0';
				if( ch > 90)
				       ch = 65;
				//intf("...%d\n", ch);
			}


			printf("writing to offset %llu\n", (n+1)*sizeof(pNodeA));



			memcpy(szBuffer, &pNodeA, sizeof(Node));
			if(!writeMMapFile(hMemory + (n+1)*sizeof(Node), szBuffer, sizeof(Node)))
                                return -1;

			void* hDestBuf = pmem_memcpy_persist(hMemory + (n+1)*sizeof(pNodeA), &pNodeA, sizeof(pNodeA));
		}*/

		printf("\nStart reading the nodes.\n");
		for( int x=nValueSizeInNode; x>=2; x=x/2)
		{
			//std::this_thread::sleep_for (std::chrono::seconds(10));

			char* szTempBuffer = new char[x+1];
			memset(szTempBuffer, 0, x);
			szTempBuffer[x] = '\0';
			
			auto tStart = std::chrono::high_resolution_clock::now();
			//auto tEnd = std::chrono::high_resolution_clock::now();
			//auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

			//for( int n=nTotalNodes-1; n>=0; n--)
			for( int n=0; n<nTotalNodes; n++)
			{
				nOffset = ((uint64_t)n)*sizeof(Node);

				//tStart = std::chrono::high_resolution_clock::now();

				if(!readMMapFile(hMemory + nOffset, szBuffer, sizeof(Node)))
					return -1;
//tStart = std::chrono::high_resolution_clock::now();

//				void* hDestBuf = pmem_memcpy(szBuffer, hMemory + nOffset, sizeof(Node), PMEM_F_MEM_NOFLUSH);
				//tEnd = std::chrono::high_resolution_clock::now();
				//tDiff += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

//        			if( hDestBuf == NULL)
//			        {			
//			                perror("Failed to read from the memory-mapped file.");
//			                return false;
//			        }
				//break;

				//Node* pNode = reinterpret_cast<Node*>(hDestBuf);
				
				//de* pNode = reinterpret_cast<Node*>(szBuffer);

				//for( int i=0; i<nTotalValuesInNode; i++)
				//for( int i=nTotalValuesInNode; i>=0; i--)
				{
				//	memcpy(szTempBuffer, &pNode->szValues[foo[i]], x);
					//printf("Read %d bytes for Node %d Index %d\n", x, n, i);
					//printf("Read %d bytes for Node %d Index %d\n", x, n, i);
					//printf("::reading..:: Node:%d, ValueIndex:%d, Value:%s\n", n+1, i, szTempBuffer/*oNode->szValues[i]*/);
				}
			}

			auto tEnd = std::chrono::high_resolution_clock::now();
			auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

			printf("Total Data Read: %llu, Read Block Size: %d, Total time diff: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);

			//std::this_thread::sleep_for(std::chrono::milliseconds(10*1000));

		}

/*
		auto tStart = std::chrono::high_resolution_clock::now();
		for( int n=0, t=sizeof(nodes)/sizeof(unsigned long long); n<t; n++) {
			//printf("::reading:: working on node %d\n", n+1);

			 if(!readMMapFile(hMemory + (n+1)*sizeof(Node), szBuffer, sizeof(Node)))
                                return -1;

			Node* pNodeB = reinterpret_cast<Node*>(szBuffer);

			for( int i=0; i<1024; i++) {
				printf("::reading:: Node:%d, ValueIndex:%d, Value:%s\n", n+1, i, pNodeB->szValues[i]);
			}

		}

		auto tEnd = std::chrono::high_resolution_clock::now();

                auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();


                printf("Total: %lld\n", tDiff);*/
		//


	//	Node* pNodeB = reinterpret_cast<Node*>(hDestBuf);

		//printf("pmem:: Version %d\n", pNodeB->nVersion);
	//	printf("pmem:: Data %s\n", pNodeB->szValues[0]);

		//memcpy(pNodeB->szData, "data changed!", strlen("data changed!"));

		//printf("pmem:: Data %s\n", pNodeB->szData);


		return 0;

		nWriteTime = 0;

		uint64_t nOffset = 0;
		uint64_t nTotalChunks = 1000;//nBytesToWrite/nBufSize;

		//auto tStart = std::chrono::high_resolution_clock::now();
		for(uint64_t nChunk=0; nChunk<nTotalChunks; nChunk++)
		{
			//printf("Chunk Id: %d.\n", nChunk);

			auto tStart = std::chrono::high_resolution_clock::now();

			if(!writeMMapFile(hMemory + nOffset, szWriteBuf, nBufSize))
				return -1;

			auto tEnd = std::chrono::high_resolution_clock::now();

			nWriteTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

			nOffset += nBufSize + 100;
		}

		 //auto tEnd = std::chrono::high_resolution_clock::now();

	         //nWriteTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();


		//printf("Write, %d, %lld\n", nBufSize, nTimeDiff);

		//printf("Started reading the blocks.\n");

		nReadTime = 0;

		nOffset = 0;
		szReadBuf = new char[nBufSize];

		//tStart = std::chrono::high_resolution_clock::now();

		for(uint64_t nChunk=0; nChunk<nTotalChunks; nChunk++)
		{
			//printf("Chunk Id: %d.\n", nChunk);
			
			auto tStart = std::chrono::high_resolution_clock::now();

			if(!readMMapFile(hMemory + nOffset, szReadBuf, nBufSize))
				return -1;

			auto tEnd = std::chrono::high_resolution_clock::now();

			/*for(size_t nIdx = 0; nIdx < nBufSize; nIdx++)
			{
				assert(szWriteBuf[nIdx] == szReadBuf[nIdx]);
			}

			memset(szReadBuf, -1, nBufSize);
			*/

			nReadTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

			nOffset += nBufSize + 100;
		}

		//tEnd = std::chrono::high_resolution_clock::now();
		//nReadTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

		printf("%d, %llu, %llu, %llu\n", nBufSize, nTotalChunks, nWriteTime/1000, nReadTime/1000);

		delete[] szWriteBuf;
		delete[] szReadBuf;

		closeMMapFile(hMemory, nMappedLen);

		std::this_thread::sleep_for(std::chrono::milliseconds(10*1000));
	}

	exit(0);
}
