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
	uint64_t nFileSize = 80*nBytesInGB;
	uint64_t nBytesToWrite = 30*nBytesInGB;
	char* szFilePath = "/mnt/pmem0fs/nvram_as_dax";

	size_t nBufSize = 1208*1024;
	//for(size_t nBufSize = 64; nBufSize < pow(2,28); nBufSize = nBufSize << 1)
	{
		//printf("Running for buffer size = %d.\n", nBufSize);

		uint64_t nWriteTime = 0;
	        uint64_t nReadTime = 0;


		szWriteBuf = new char[nBufSize];
		for(size_t nIdx= 0; nIdx<nBufSize; nIdx++)
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

		uint64_t nOffset = 35*nBytesInGB, nOffsetII = 75*nBytesInGB;
		uint64_t nTotalChunks =  nBytesToWrite/nBufSize;
		auto tStart = std::chrono::high_resolution_clock::now();

		/*for(size_t nChunk = 0; nChunk < nTotalChunks; nChunk++)
		{
			if( nChunk%2==0) {
				//printf("even %ul\n", nOffset);
				writeMMapFile(hMemory + nOffset, szWriteBuf, nBufSize);
				nOffset -= (nBufSize * 2);
			}
			else {
				//printf("odd %ul\n", nOffsetII);
			  	writeMMapFile(hMemory + nOffsetII, szWriteBuf, nBufSize);
                                nOffsetII -= (nBufSize * 2);
			}
		}*/

		auto tEnd = std::chrono::high_resolution_clock::now();

                nWriteTime = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();
		std::this_thread::sleep_for(std::chrono::milliseconds(60*1000));
		tStart = std::chrono::high_resolution_clock::now();


		nOffset = 35*nBytesInGB, nOffsetII = 75*nBytesInGB;
		szReadBuf = new char[nBufSize];
		for(size_t nChunk = 0; nChunk < nTotalChunks; nChunk++)
		{
			if (nChunk%2 == 0) {
				readMMapFile(hMemory + nOffset, szReadBuf, nBufSize);
				nOffset -= (nBufSize*2);
			} else {
				readMMapFile(hMemory + nOffsetII, szReadBuf, nBufSize);
                                nOffsetII -= (nBufSize*2);
			}
		}

		tEnd = std::chrono::high_resolution_clock::now();
                nReadTime = std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

               
                printf("%d, %llu, %lu, %lu\n", nBufSize, nTotalChunks, nWriteTime, nReadTime);


		delete[] szReadBuf;
		delete[] szWriteBuf;

		closeMMapFile(hMemory, nMappedLen);

		std::this_thread::sleep_for(std::chrono::milliseconds(60*1000));
	}

	exit(0);
}
