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

void closeMMapFile(void* hMemory, size_t nMappedLen)
{
        pmem_unmap(hMemory, nMappedLen);
}


#include <iostream>     // std::cout
#include <algorithm>    // std::shuffle
#include <array>        // std::array
#include <random>       // std::default_random_engine
#include <chrono>

const int nTotalNodes = 1024;
const int nValueSizeInNode = 4096;
const int nTotalValuesInNode = 1024;

struct Node {
	char szValues[nTotalValuesInNode][nValueSizeInNode];
};


//#define _VALIDATE_

void dram_sequential()
{
	void* hMemory =  operator new(nTotalNodes*sizeof(Node));
	
	printf("Start writing the data.\n");
	
	for( int n=0; n<nTotalNodes; n++) 
	{
                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++)
		{
			ch = ch > 90 ? 65 : ch;
                        memset(&oNode.szValues[i], ch, nValueSizeInNode);
                        //oNode.szValues[i][nValueSizeInNode-1]='\0';
                }
		
		void* hDestBuf = memcpy(hMemory + ((uint64_t)n)*sizeof(Node), &oNode, sizeof(Node));

                if( hDestBuf == NULL)
                {
                        printf("Failed to copy the data completely.");
                        return;
                }
        }

	printf("Finish writing the data.\n");

        printf("Start reading the nodes.\n");

        for( int x=nValueSizeInNode; x>=2; x=x/2)
        {
		//std::this_thread::sleep_for (std::chrono::seconds(10));

                /*for( int n=0; n<nTotalNodes; n++)
                {
                        Node* oNode = reinterpret_cast<Node*>(hMemory + n*sizeof(Node));

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                oNode->szValues[i][x-1] = '\0';
                        }
                }*/

                char* szTempBuffer = new char[x+1];
                memset(szTempBuffer, 0, x+1);
                //szTempBuffer[x+1] = '\0';
		
                auto tStart = std::chrono::high_resolution_clock::now();

                for( int n=0; n<nTotalNodes; n++)
                {
                        Node* oNode = reinterpret_cast<Node*>(hMemory + ((uint64_t)n)*sizeof(Node));

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                memcpy(szTempBuffer, &oNode->szValues[i], x);

#ifdef _VALIDATE_
				char* szCompare = new char[x+1];
				memset(szCompare, 65 + i%26, x);
				szCompare[x]= '\0';

				if( strcmp(szCompare, szTempBuffer) != 0)
				{
					printf("\n(%d %d)\n->[%s]\n->[%s]\n", strlen(szCompare), strlen(szTempBuffer), szCompare, szTempBuffer);
					exit(0);
				}
				
				delete[] szCompare;
#endif //_VALIDATE_
			}
                }

                auto tEnd = std::chrono::high_resolution_clock::now();
                auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                printf("Total Data Read: %llu, Read Block Size: %d, Total time: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);

		delete[] szTempBuffer;
        }

	delete[] hMemory;
}

void dram_random(std::array<int,1024>& foo)
{
        void* hMemory =  operator new(nTotalNodes*sizeof(Node));

        printf("Start writing the data.\n");

        for( int n=0; n<nTotalNodes; n++)
        {
                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++) 
		{
			ch = ch > 90 ? 65 : ch;
                        memset(&oNode.szValues[i], ch, nValueSizeInNode);
                        //oNode.szValues[i][nValueSizeInNode-1]='\0';
                }

                void* hDestBuf = memcpy(hMemory + n*sizeof(Node), &oNode, sizeof(Node));

                if( hDestBuf == NULL)
                {
                        printf("Failed to copy the data completely.");
                        return;
                }
        }

        printf("Finish writing the data.\n");

        printf("Start reading the nodes.\n");

        for( int x=nValueSizeInNode; x>=2; x=x/2)
        {
                //std::this_thread::sleep_for (std::chrono::seconds(10));

                /*for( int n=0; n<nTotalNodes; n++)
                {
                        Node* oNode = reinterpret_cast<Node*>(hMemory + n*sizeof(Node));

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                oNode->szValues[i][x-1] = '\0';
                        }
                }*/

                char* szTempBuffer = new char[x+1];
                memset(szTempBuffer, 0, x+1);
                //szTempBuffer[x-1] = '\0';

                auto tStart = std::chrono::high_resolution_clock::now();

                for( int n=0; n<nTotalNodes; n++)
                {
                        Node* oNode = reinterpret_cast<Node*>(hMemory + foo[n]*sizeof(Node));

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                memcpy(szTempBuffer, &oNode->szValues[foo[i]], x);

#ifdef _VALIDATE_
                                char* szCompare = new char[x+1];
                                memset(szCompare, 65 + foo[i]%26, x);
                                szCompare[x]= '\0';

                                if( strcmp(szCompare, szTempBuffer) != 0)
                                {
                                        printf("\n(%d %d)\n->[%s]\n->[%s]\n", strlen(szCompare), strlen(szTempBuffer), szCompare, szTempBuffer);
                                        exit(0);
                                }

                                delete[] szCompare;
#endif //_VALIDATE_

                        }
                }

                auto tEnd = std::chrono::high_resolution_clock::now();
                auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                printf("Total Data Read: %llu, Read Block Size: %d, Total time: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);

                delete[] szTempBuffer;
        }

        delete[] hMemory;
}

void direct_sequential()
{
	int nIsPMem;
        size_t nMappedLen;
        void* hMemory = NULL;
        //char* szReadBuf = NULL;
        //char* szWriteBuf = NULL;
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

        printf("Start writing the data.\n");

        char* szBuffer = new char[sizeof(Node)];
        for( int n=0; n<nTotalNodes; n++)
        {
                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++)
                {
			ch = ch > 90 ? 65 : ch;
                        memset(&oNode.szValues[i], ch, nValueSizeInNode);
                        //oNode.szValues[i][nValueSizeInNode-1]='\0';
                }

                memcpy(szBuffer, &oNode, sizeof(Node));

                if(!writeMMapFile(hMemory + n*sizeof(Node), szBuffer, sizeof(Node)))
                {
                        perror("Failed to write the data completely.");
                        exit(1);
                }

        }

        printf("Finished writing the data.\n");

	printf("Start reading the nodes.\n");

	for( int x=nValueSizeInNode; x>=2; x=x/2) 
	{
		//std::this_thread::sleep_for (std::chrono::seconds(10));

		/*for( int n=0; n<nTotalNodes; n++)
                {
			//nOffset = ((uint64_t)n)*sizeof(Node);

                        Node* oNode = reinterpret_cast<Node*>(hMemory + n*sizeof(Node));

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                oNode->szValues[i][x-1] = '\0';
			}
                }*/

		char* szTempBuffer = new char[x+1];
		memset(szTempBuffer, 0, x+1);
		//szTempBuffer[x-1] = '\0';

		uint64_t sum = 0;		

		auto tStart = std::chrono::high_resolution_clock::now();

       		for( int n=0; n<nTotalNodes; n++)
		{
			//nOffset = ((uint64_t)n)*sizeof(Node);
			Node* oNode = reinterpret_cast<Node*>(hMemory + n*sizeof(Node));
			
			for( int i=0; i<nTotalValuesInNode; i++)
			{
				pmem_memcpy(szTempBuffer, &oNode->szValues[i], x, PMEM_F_MEM_NOFLUSH);

#ifdef _VALIDATE_
                                char* szCompare = new char[x+1];
                                memset(szCompare, 65 + i%26, x);
                                szCompare[x]= '\0';

                                if( strcmp(szCompare, szTempBuffer) != 0)
                                {
                                        printf("\n(%d %d)\n->[%s]\n->[%s]\n", strlen(szCompare), strlen(szTempBuffer), szCompare, szTempBuffer);
                                        exit(0);
                                }

                                delete[] szCompare;
#endif //_VALIDATE_

			}
         	}

	        auto tEnd = std::chrono::high_resolution_clock::now();
        	auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

		printf("Total Data Read: %llu, Read Block Size: %d, Total time: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);
	}
	
        delete[] szBuffer;

        closeMMapFile(hMemory, nMappedLen);
}

void direct_random(std::array<int,1024>& foo)
{
        int nIsPMem;
        size_t nMappedLen;
        void* hMemory = NULL;
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

        printf("Start writing the data.\n");

        char* szBuffer = new char[sizeof(Node)];
        for( int n=0; n<nTotalNodes; n++)
        {
                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++)
                {
			ch = ch > 90 ? 65 : ch;
                        memset(&oNode.szValues[i], ch, nValueSizeInNode);
                        //oNode.szValues[i][nValueSizeInNode-1]='\0';
                }

                memcpy(szBuffer, &oNode, sizeof(Node));

                if(!writeMMapFile(hMemory + n*sizeof(Node), szBuffer, sizeof(Node)))
                {
                        perror("Failed to write the data completely.");
                        exit(1);
                }

        }

        printf("Finished writing the data.\n");

        printf("Start reading the nodes.\n");

        for( int x=nValueSizeInNode; x>=2; x=x/2)
        {
                //std::this_thread::sleep_for (std::chrono::seconds(10));

                /*for( int n=0; n<nTotalNodes; n++)
                {
                        Node* oNode = reinterpret_cast<Node*>(hMemory + n*sizeof(Node));

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                oNode->szValues[i][x-1] = '\0';
                        }
                }*/

                char* szTempBuffer = new char[x+1];
                memset(szTempBuffer, 0, x+1);
                //szTempBuffer[x-1] = '\0';

                uint64_t sum = 0;

                auto tStart = std::chrono::high_resolution_clock::now();

                for( int n=0; n<nTotalNodes; n++)
                {
                        Node* oNode = reinterpret_cast<Node*>(hMemory + foo[n]*sizeof(Node));

                        for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                pmem_memcpy(szTempBuffer, &oNode->szValues[foo[i]], x, PMEM_F_MEM_NOFLUSH);

#ifdef _VALIDATE_
                                char* szCompare = new char[x+1];
                                memset(szCompare, 65 + foo[i]%26, x);
                                szCompare[x]= '\0';

                                if( strcmp(szCompare, szTempBuffer) != 0)
                                {
                                        printf("\n(%d %d)\n->[%s]\n->[%s]\n", strlen(szCompare), strlen(szTempBuffer), szCompare, szTempBuffer);
                                        exit(0);
                                }

                                delete[] szCompare;
#endif //_VALIDATE_

                        }
                }

                auto tEnd = std::chrono::high_resolution_clock::now();
                auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                printf("Total Data Read: %llu, Read Block Size: %d, Total time: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);
        }

        delete[] szBuffer;

        closeMMapFile(hMemory, nMappedLen);
}

void in_direct_I_sequential()
{
	int nIsPMem;
	size_t nMappedLen;
	void* hMemory = NULL;
	uint64_t nBytesInGB = 1024*1024*1024;
	uint64_t nFileSize = 10*nBytesInGB;
	uint64_t nBytesToWrite = 5*nBytesInGB;
	char* szFilePath = "/home/wuensche/pool/pool0";

	uint64_t nOffset = 0;

	if( !openMMapFile(hMemory, szFilePath, nMappedLen, nIsPMem))
	{
		if( !createMMapFile(hMemory, szFilePath, nFileSize, nMappedLen, nIsPMem))
		{
			exit(1);
		}
	}

	printf("Start writing the data.\n");

	char* szBuffer = new char[sizeof(Node)];
        for( int n=0; n<nTotalNodes; n++)
        {
                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++)
                {
			ch = ch > 90 ? 65 : ch;
                        memset(&oNode.szValues[i], ch, nValueSizeInNode-1);
                        oNode.szValues[i][nValueSizeInNode-1]='\0';
                }

                memcpy(szBuffer, &oNode, sizeof(Node));
                        
                if(!writeMMapFile(hMemory + n*sizeof(Node), szBuffer, sizeof(Node)))
                {
                        perror("Failed to write the data completely.");
                        exit(1);
                }
                
        }

        printf("Finished writing the data.\n");

	printf("Start reading the data.\n");

	char* szTempBuffer = new char[sizeof(Node)];

	for( int x=nValueSizeInNode; x>=2; x=x/2)
	{
		//std::this_thread::sleep_for (std::chrono::seconds(10));

		auto tStart = std::chrono::high_resolution_clock::now();

		for( int n=0; n<nTotalNodes; n++)
		{
			nOffset = ((uint64_t)n)*sizeof(Node);

			if(!readMMapFile(hMemory + nOffset, szTempBuffer, sizeof(Node)))
			{
				printf("Failed to read the data completely!\n");
				return;
			}

#ifdef _VALIDATE_
			if( strncmp(szBuffer, szTempBuffer, sizeof(Node)) != 0)
			{
				printf("\n->[%s]\n->[%s]\n", szBuffer, szTempBuffer);
				exit(0);
                        }
#endif //_VALIDATE_
		}

		auto tEnd = std::chrono::high_resolution_clock::now();
		auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

		printf("Total Data Read: %llu, Total time: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, tDiff);
	}

	delete[] szBuffer;
	delete[] szTempBuffer;

	closeMMapFile(hMemory, nMappedLen);

	return;
}

void in_direct_I_random(std::array<int,1024>& foo)
{
        int nIsPMem;
        size_t nMappedLen;
        void* hMemory = NULL;
        uint64_t nBytesInGB = 1024*1024*1024;
        uint64_t nFileSize = 10*nBytesInGB;
        uint64_t nBytesToWrite = 5*nBytesInGB;
        char* szFilePath = "/home/wuensche/pool/pool0";

        uint64_t nOffset = 0;

        if( !openMMapFile(hMemory, szFilePath, nMappedLen, nIsPMem))
        {
                if( !createMMapFile(hMemory, szFilePath, nFileSize, nMappedLen, nIsPMem))
                {
                        exit(1);
                }
        }

        printf("Start writing the data.\n");

	char* szBuffer = new char[sizeof(Node)];
        for( int n=0; n<nTotalNodes; n++)
        {
                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++)
                {
			ch = ch > 90 ? 65 : ch;
                        memset(&oNode.szValues[i], ch, nValueSizeInNode-1);
                        oNode.szValues[i][nValueSizeInNode-1]='\0';
                }

                memcpy(szBuffer, &oNode, sizeof(Node));
                        
                if(!writeMMapFile(hMemory + n*sizeof(Node), szBuffer, sizeof(Node)))
                {
                        perror("Failed to write the data completely.");
                        exit(1);
                }
                
        }

        printf("Finished writing the data.\n");

	char* szTempBuffer = new char[sizeof(Node)];

        printf("Start reading the data.\n");

        for( int x=nValueSizeInNode; x>=2; x=x/2)
        {
                //std::this_thread::sleep_for (std::chrono::seconds(10));

                auto tStart = std::chrono::high_resolution_clock::now();

                for( int n=0; n<nTotalNodes; n++)
                {
                        nOffset = ((uint64_t)foo[n])*sizeof(Node);

                        if(!readMMapFile(hMemory + nOffset, szTempBuffer, sizeof(Node)))
                        {
                                printf("Failed to read the data completely!\n");
                                return;
                        }

#ifdef _VALIDATE_
                        if( strncmp(szBuffer, szTempBuffer, sizeof(Node)) != 0)
                        {
                                printf("\n->[%s]\n->[%s]\n", szBuffer, szTempBuffer);
                                exit(0);
                        }
#endif //_VALIDATE_
                }

                auto tEnd = std::chrono::high_resolution_clock::now();
                auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                printf("Total Data Read: %llu, Total time: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, tDiff);
        }

	delete[] szBuffer;
	delete[] szTempBuffer;

	closeMMapFile(hMemory, nMappedLen);

        return;
}

void in_direct_II_sequential()
{
        int nIsPMem;
        size_t nMappedLen;
        void* hMemory = NULL;
        uint64_t nBytesInGB = 1024*1024*1024;
        uint64_t nFileSize = 10*nBytesInGB;
        uint64_t nBytesToWrite = 5*nBytesInGB;
        char* szFilePath = "/home/wuensche/pool/pool0";

        uint64_t nOffset = 0;

        if( !openMMapFile(hMemory, szFilePath, nMappedLen, nIsPMem))
        {
                if( !createMMapFile(hMemory, szFilePath, nFileSize, nMappedLen, nIsPMem))
                {
                        exit(1);
                }
        }

	printf("Start writing the data.\n");

        char* szBuffer = new char[sizeof(Node)];
        for( int n=0; n<nTotalNodes; n++)
        {
                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++)
                {
			ch = ch > 90 ? 65 : ch;
                        memset(&oNode.szValues[i], ch, nValueSizeInNode-1);
                        oNode.szValues[i][nValueSizeInNode-1]='\0';
                }

                memcpy(szBuffer, &oNode, sizeof(Node));

                if(!writeMMapFile(hMemory + n*sizeof(Node), szBuffer, sizeof(Node)))
                {
                        perror("Failed to write the data completely.");
                        exit(1);
                }

        }

        printf("Finished writing the data.\n");

        printf("Start reading the data.\n");

        for( int x=nValueSizeInNode; x>=2; x=x/2)
        {
                //std::this_thread::sleep_for (std::chrono::seconds(10));

                char* szTempBuffer = new char[x];
                memset(szTempBuffer, 0, x);
                szTempBuffer[x-1] = '\0';

                auto tStart = std::chrono::high_resolution_clock::now();

                for( int n=0; n<nTotalNodes; n++)
                {
                        nOffset = ((uint64_t)n)*sizeof(Node);

                        if(!readMMapFile(hMemory + nOffset, szBuffer, sizeof(Node)))
                        {
                                printf("Failed to read the data completely!\n");
                                return;
                        }

                        Node* pNode = reinterpret_cast<Node*>(szBuffer);
			
			for( int i=0; i<nTotalValuesInNode; i++)
                        {
                                memcpy(szTempBuffer, &pNode->szValues[i], x-1);

#ifdef _VALIDATE_
                                char* szCompare = new char[x];
                                memset(szCompare, 65 + i%26, x);
                                szCompare[x-1]= '\0';

                                if( strcmp(szCompare, szTempBuffer) != 0)
                                {
                                        printf("\n(%d %d)\n->[%s]\n->[%s]\n", strlen(szCompare), strlen(szTempBuffer), szCompare, szTempBuffer);
                                        exit(0);
                                }

                                delete[] szCompare;
#endif //_VALIDATE_

                        }
                }

                auto tEnd = std::chrono::high_resolution_clock::now();
                auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                printf("Total Data Read: %llu, Read Block Size: %d, Total time: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);

		delete[] szTempBuffer;
        }

	delete[] szBuffer;

	closeMMapFile(hMemory, nMappedLen);

        return;
}

void in_direct_II_random(std::array<int,1024>& foo)
{
        int nIsPMem;
        size_t nMappedLen;
        void* hMemory = NULL;
        uint64_t nBytesInGB = 1024*1024*1024;
        uint64_t nFileSize = 10*nBytesInGB;
        uint64_t nBytesToWrite = 5*nBytesInGB;
        char* szFilePath = "/home/wuensche/pool/pool0";

        uint64_t nOffset = 0;

        if( !openMMapFile(hMemory, szFilePath, nMappedLen, nIsPMem))
        {
                if( !createMMapFile(hMemory, szFilePath, nFileSize, nMappedLen, nIsPMem))
                {
                        exit(1);
                }
        }

	printf("Start writing the data.\n");

        char* szBuffer = new char[sizeof(Node)];
        for( int n=0; n<nTotalNodes; n++)
        {
                Node oNode;
                for( int i=0, ch=65; i<nTotalValuesInNode; i++, ch++)
                {
			ch = ch > 90 ? 65 : ch;
                        memset(&oNode.szValues[i], ch, nValueSizeInNode-1);
                        oNode.szValues[i][nValueSizeInNode-1]='\0';
                }

                memcpy(szBuffer, &oNode, sizeof(Node));

                if(!writeMMapFile(hMemory + n*sizeof(Node), szBuffer, sizeof(Node)))
                {
                        perror("Failed to write the data completely.");
                        exit(1);
                }

        }

        printf("Finished writing the data.\n");

        printf("Start reading the data.\n");

        for( int x=nValueSizeInNode; x>=2; x=x/2)
        {
		//std::this_thread::sleep_for (std::chrono::seconds(10));
		
		char* szTempBuffer = new char[x];
		memset(szTempBuffer, 0, x);
		szTempBuffer[x-1] = '\0';

                auto tStart = std::chrono::high_resolution_clock::now();

                for( int n=0; n<nTotalNodes; n++)
                {
                        nOffset = ((uint64_t)foo[n])*sizeof(Node);

                        if(!readMMapFile(hMemory + nOffset, szBuffer, sizeof(Node)))
                        {
                                printf("Failed to read the data completely!\n");
                                return;
                        }

			Node* pNode = reinterpret_cast<Node*>(szBuffer);
			
			for( int i=0; i<nTotalValuesInNode; i++)
			{
				memcpy(szTempBuffer, &pNode->szValues[foo[i]], x-1);

#ifdef _VALIDATE_
                                char* szCompare = new char[x];
                                memset(szCompare, 65 + foo[i]%26, x);
                                szCompare[x-1]= '\0';

                                if( strcmp(szCompare, szTempBuffer) != 0)
                                {
                                        printf("\n(%d %d)\n->[%s]\n->[%s]\n", strlen(szCompare), strlen(szTempBuffer), szCompare, szTempBuffer);
                                        exit(0);
                                }

                                delete[] szCompare;
#endif //_VALIDATE_

			}
                }

                auto tEnd = std::chrono::high_resolution_clock::now();
                auto tDiff = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

                printf("Total Data Read: %llu, Read Block Size: %d, Total time: %lld\n", ((uint64_t)nTotalNodes)*nTotalValuesInNode*x, x, tDiff);

		delete[] szTempBuffer;
        }

	delete[] szBuffer;

	closeMMapFile(hMemory, nMappedLen);

        return;
}

int main(int argc, char *argv[])
{
        std::array<int,1024> foo;
        for( int idx = 0; idx < 1024; idx++) {
                foo[idx] = idx;
        }

        // obtain a time-based seed:
        unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	shuffle (foo.begin(), foo.end(), std::default_random_engine(seed));

        printf("dram_sequential\n");
        std::this_thread::sleep_for (std::chrono::seconds(5));
        dram_sequential();
        printf("dram_random\n");
        std::this_thread::sleep_for (std::chrono::seconds(5));
        dram_random(foo);

        printf("direct_sequential\n");
	std::this_thread::sleep_for (std::chrono::seconds(5));
        direct_sequential();
	printf("direct_random\n");
        std::this_thread::sleep_for (std::chrono::seconds(5));
        direct_random(foo);
        
	printf("in_direct_I_sequential\n");
	std::this_thread::sleep_for (std::chrono::seconds(5));
	in_direct_I_sequential();
        printf("in_direct_I_random\n");
        std::this_thread::sleep_for (std::chrono::seconds(5));
        in_direct_I_random(foo);
        printf("in_direct_II_sequential\n");
        std::this_thread::sleep_for (std::chrono::seconds(5));
	in_direct_II_sequential();
        printf("in_direct_II_random\n");
        std::this_thread::sleep_for (std::chrono::seconds(5));
	in_direct_II_random(foo);
}

