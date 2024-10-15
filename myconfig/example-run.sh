./storage_server -a 0.0.0.0:50051 -d ../mydata/testdb
./memory_server -c ../myconfig/memory.config
./orderer -l -a 0.0.0.0:50053 -c ../myconfig/consensus.config
./orderer -a 0.0.0.0:50053 -c ../myconfig/consensus.config
./compute_server -v -a 0.0.0.0:50055 -c ../myconfig/compute.config