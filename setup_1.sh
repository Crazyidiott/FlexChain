export CLOUDLAB_HOME=~/opt/dahui
export MY_INSTALL_DIR=$CLOUDLAB_HOME/.local
export PATH="$MY_INSTALL_DIR/bin:$PATH"

# setup RDMA
sudo apt-get update
sudo apt-get install -y rdma-core ibverbs-utils librdmacm1 libibverbs1 ibverbs-providers rdmacm-utils libibverbs-dev libosmvendor5 libosmcomp5 libopensm9

sudo systemctl start iwpmd.service
sudo systemctl start rdma-hw.target
sudo systemctl start rdma-ndd.service

sudo modprobe rdma_ucm
sudo modprobe ib_uverbs
sudo modprobe ib_umad
sudo modprobe ib_ipoib

# install libs
sudo apt install -y cmake build-essential autoconf libtool pkg-config libssl-dev libsystemd-dev

# build levelDB
cd $CLOUDLAB_HOME
git clone --recurse-submodules https://github.com/google/leveldb.git
cd leveldb
mkdir -p build && cd build
# cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 .. && cmake --build .
sudo make install
