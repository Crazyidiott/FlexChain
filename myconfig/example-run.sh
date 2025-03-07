sudo ./storage_server -a 0.0.0.0:50051 -d ../mydata/testdb
sudo ./memory_server -c ../myconfig/memory.config
sudo ./orderer -l -a 0.0.0.0:50051 -c ../myconfig/consensus.config
sudo ./orderer -f -a 0.0.0.0:50051 -c ../myconfig/consensus.config
sudo ./compute_server -v -a 0.0.0.0:50051 -c ../myconfig/compute.config