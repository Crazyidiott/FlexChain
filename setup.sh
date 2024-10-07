export CLOUDLAB_HOME=/opt/Sicong
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
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
sudo make install

# build gRPC
cd $CLOUDLAB_HOME
mkdir -p $MY_INSTALL_DIR
git clone --recurse-submodules -b v1.66.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
cd grpc
mkdir -p cmake/build
cd cmake/build
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DgRPC_INSTALL=ON \
  -DgRPC_BUILD_TESTS=OFF \
  -DgRPC_SSL_PROVIDER=package \
  -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
  ../..
make -j 64
make install

# build FlexChain
cd $CLOUDLAB_HOME/FlexChain
mkdir -p build && cd build
cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ..
make -j 4
