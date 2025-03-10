export CLOUDLAB_HOME=/opt/dahui
export MY_INSTALL_DIR=$CLOUDLAB_HOME/.local
export PATH="$MY_INSTALL_DIR/bin:$PATH"

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