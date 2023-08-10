g++ dimes_23.cpp -o dimes_23 -lpmem  -pthread

numactl -N <0 or 1> ./dimes_23



