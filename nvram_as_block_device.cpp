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

#define BLOCK_SIZE 4096

int main(int argc, char *argv[])
{
	int hFile;
	char* szReadBuf = new char[BLOCK_SIZE];
	char* szWriteBuf = new char[BLOCK_SIZE];
	char* szFilePath = "/mnt/pmemfs0/nvram_as_blk_dvc.txt";

        if((hFile = open(szFilePath, O_RDWR | O_CREAT | O_DIRECT | O_DSYNC)) < 0) 
	{
		perror("Failed to create/open the file.");
       	        return -1;
	}

	for(size_t nIdx=0; nIdx<BLOCK_SIZE; nIdx++)
	{
		szWriteBuf[nIdx] = (rand() % 10);
	}

	if(write(hFile, szWriteBuf, BLOCK_SIZE) != BLOCK_SIZE) 
	{
		perror("Failed to write to the file.");
		return -1;
	}

	sync();
	
	size_t nOffset = lseek(hFile, 0, SEEK_SET);

        if(read(hFile, szReadBuf, BLOCK_SIZE) != BLOCK_SIZE){
		perror("Failed to read from the file.");
		return -1;
	}

	delete[] szReadBuf;
	delete[] szWriteBuf;

	close(hFile);

	return 0;
}
