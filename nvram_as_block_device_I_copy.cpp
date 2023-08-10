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

/* just copying 4k to pmem for this example */
#define BUF_LEN 512
#define	POOL_SIZE 100*1024*1024*1024

bool mmap_file(void*& hMemory, const char* szPath, size_t& nMappedLen, int& bIsPMem)
{
	uint64_t filesize = (uint64_t) 10*1024*1024*1024;
        if ((hMemory = pmem_map_file(szPath,
                                filesize,
                                PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                                0666, &nMappedLen, &bIsPMem)) == NULL) 
	{
                perror("Failed to memory map the persistent memory.");
                return false;
        }

	return true;
}

bool open_mmap_file(void*& hMemory, const char* szPath, size_t& nMappedLen, int& bIsPMem)
{
        if ((hMemory = pmem_map_file(szPath,
                                0,
                                0,
                                0666, &nMappedLen, &bIsPMem)) == NULL)
        {
                perror("Failed to open memory-mapped file for persistent memory.");
                return false;
        }

        return true;
}

bool mmap_file_write(void* hMemory, const char* szBuf, size_t nLen)
{
	void* hDestBuf = pmem_memcpy_persist(hMemory, szBuf, nLen);

	if( hDestBuf == NULL)
		return false;

	return true;
}

bool mmap_file_read(const void* hMemory, char* szBuf, size_t nLen)
{
        void* hDestBuf = pmem_memcpy(szBuf, hMemory, nLen, PMEM_F_MEM_NOFLUSH);
	//memcpy(szBuf, hMemory, nLen);

        if( hDestBuf == NULL)
                return false;

        return true;
}

void close_mmap_file(void* hMemory, size_t nMappedLen)
{
	pmem_unmap(hMemory, nMappedLen);
}

int main(int argc, char *argv[])
{
	if (argc != 2) {
		fprintf(stderr, "Usage: %s file path in persistent memory.\n", argv[0]);
		exit(1);
	}

	int srcfd;
	size_t nBytesRead = 0;
	size_t nBytesWritten = 0;

	int nIsPMem;
	size_t nMappedLen;
	void* hMemory = NULL;

	char* szBuf = NULL;
	char* szReadBuf = NULL;
	size_t nBufSize = 64;
	uint64_t nBytesToWrite = (uint64_t)5*1024*1024*1024; 

	uint64_t nWriteTime = 0;
	uint64_t nReadTime = 0;

	for(size_t nBufSize = 64; nBufSize < pow(2,28); nBufSize = nBufSize << 1)
	{
		//printf("Running for buffer size = %d.\n", nBufSize);

		szBuf = new char[nBufSize];
		for(size_t index  = 0; index < nBufSize; index++)
		{
        		szBuf[index] = (rand() % 10);
	     	}	

	        if ((srcfd = open(argv[1], O_RDWR | O_CREAT | O_DIRECT | O_DSYNC)) < 0) {
	                perror(argv[1]);
        	        exit(1);
	        }


		//printf("Started writing the blocks.\n");

		nWriteTime = 0;

		 auto tStart = std::chrono::high_resolution_clock::now();
		uint64_t nOffset = 0;
		uint64_t nTotalChunks =  nBytesToWrite/nBufSize;
		for(size_t nChunk = 0; nChunk < nTotalChunks; nChunk++)
		{
			printf("Chunk Id: %d.\n", nChunk);

			//auto tStart = std::chrono::high_resolution_clock::now();

			if ((nBytesWritten = write(srcfd, szBuf, nBufSize)) != nBufSize) 
			{
                		perror("write");
		                exit(1);
		        }

			sync();

			//auto tEnd = std::chrono::high_resolution_clock::now();

			//nWriteTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

			nOffset += nBufSize;
		}
		 auto tEnd = std::chrono::high_resolution_clock::now();

                        nWriteTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();


		//printf("Write, %d, %lld\n", nBufSize, nTimeDiff);

		//printf("Started reading the blocks.\n");

		size_t x = lseek(srcfd, 0, SEEK_SET);

		nReadTime = 0;

		tStart = std::chrono::high_resolution_clock::now();


		nOffset = 0;
		szReadBuf = new char[nBufSize];
		for(size_t nChunk = 0; nChunk < nTotalChunks; nChunk++)
		{
			//printf("Chunk Id: %d.\n", nChunk);
			
			//auto tStart = std::chrono::high_resolution_clock::now();

		        if ((nBytesRead = read(srcfd, szReadBuf, nBufSize)) != nBufSize){
                		perror("read");
		                exit(1);
		        }

			//auto tEnd = std::chrono::high_resolution_clock::now();

			/*for(size_t nIdx = 0; nIdx < nBufSize; nIdx++)
			{
				assert(szBuf[nIdx] == szReadBuf[nIdx]);
			}*/

			//nReadTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

			nOffset += nBufSize;
		}

		tEnd = std::chrono::high_resolution_clock::now();
		nReadTime += std::chrono::duration<uint64_t, std::nano>(tEnd-tStart).count();

		printf("%d, %lld, %lld, %lld\n", nBufSize, nTotalChunks, nWriteTime, nReadTime);

		delete[] szBuf;
		delete[] szReadBuf;

		close(srcfd);

		std::this_thread::sleep_for(std::chrono::milliseconds(10*1000));
	}

	exit(0);
}
