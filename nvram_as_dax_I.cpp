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

int main(int argc, char *argv[])
{
	int nIsPMem;
	size_t nMappedLen;
	void* hMemory = NULL;
	char* szReadBuf = NULL;
	char* szWriteBuf = NULL;
	uint64_t nBytesInGB = 1024*1024*1024;
	uint64_t nFileSize = 50*nBytesInGB;
	uint64_t nBytesToWrite = 5*nBytesInGB;
	char* szFilePath = "/mnt/pmem_fs0/haura/pmempool0";

	uint64_t nWriteTime = 0;
	uint64_t nReadTime = 0;

	std::chrono::time_point<std::chrono::high_resolution_clock> tStart, tEnd;

	for(size_t nBufSize=64; nBufSize<pow(2,28); nBufSize=nBufSize<<1)
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

		if (!nIsPMem) 
		{
			perror("Not a persistent memory module.");
			exit(1);
		}

	//	printf("Started writing the blocks. %d\n", nMappedLen);

		nWriteTime = 0;

		uint64_t nOffset = 0;
		uint64_t nTotalChunks = nBytesToWrite/nBufSize;

		uint64_t nChunk=0;
		tStart = std::chrono::high_resolution_clock::now();
		for(/*uint64_t nChunk=0*/; nChunk<nTotalChunks; nChunk++)
		{
			//printf("Chunk Id: %d.\n", nChunk);

			//tStart = std::chrono::high_resolution_clock::now();

			//if(!writeMMapFile(hMemory + nOffset, szWriteBuf, nBufSize))
			//	return -1;
			 void* hDestBuf = pmem_memcpy_persist(static_cast<char*>(hMemory) + nOffset, szWriteBuf, nBufSize);

		        if( hDestBuf == NULL)
		        {
		                perror("Failed to write to the memory-mapped file.");
		                return -1;
		        }


			//tEnd = std::chrono::high_resolution_clock::now();

			//nWriteTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

			nOffset += nBufSize;
		}

		tEnd = std::chrono::high_resolution_clock::now();

	        nWriteTime = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

 std::this_thread::sleep_for(std::chrono::milliseconds(10*1000));

		//printf("Write, %d, %lld\n", nBufSize, nTimeDiff);
//printf("Chunk Id: %lu.\n", nChunk);

//		printf("Started reading the blocks.\n");

		nReadTime = 0;

		nOffset = 0;
		szReadBuf = new char[nBufSize];

		tStart = std::chrono::high_resolution_clock::now();

		for(/*uint64_t*/ nChunk=0; nChunk<nTotalChunks; nChunk++)
		{
			//printf("Chunk Id: %lu.\n", nChunk);
			
			//tStart = std::chrono::high_resolution_clock::now();

			//if(!readMMapFile(hMemory + nOffset, szReadBuf, nBufSize))
			//	return -1;
			void* hDestBuf = pmem_memcpy(szReadBuf, static_cast<char*>(hMemory) + nOffset, nBufSize, PMEM_F_MEM_NOFLUSH);

		        if( hDestBuf == NULL)
		        {
		                perror("Failed to read from the memory-mapped file.");
		                return -1;
		        }


			//tEnd = std::chrono::high_resolution_clock::now();

			/*for(size_t nIdx = 0; nIdx < nBufSize; nIdx++)
			{
				assert(szWriteBuf[nIdx] == szReadBuf[nIdx]);
			}

			memset(szReadBuf, -1, nBufSize);
			*/

			//nReadTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

			nOffset += nBufSize;
		}

		tEnd = std::chrono::high_resolution_clock::now();
		nReadTime = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();
//printf("Chunk Id: %lu.\n", nChunk);

		printf("%lu, %lu\n", nWriteTime, nReadTime);
		//printf("%d, %llu, %d, %d\n", nBufSize, nTotalChunks, nWriteTime, nReadTime);


		delete[] szWriteBuf;
		delete[] szReadBuf;

		closeMMapFile(hMemory, nMappedLen);

		std::this_thread::sleep_for(std::chrono::milliseconds(10*1000));
	}

	exit(0);
}
