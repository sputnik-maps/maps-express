FROM ubuntu:xenial-20170619

WORKDIR /tmp

RUN apt-get update

RUN apt-get install -y git cmake gcc g++ libgoogle-glog-dev libjsoncpp-dev libicu-dev \
    libgflags-dev zlib1g-dev libdouble-conversion-dev libevent-dev protobuf-compiler \
    libprotobuf-dev libprotozero-dev libtool pkg-config libssl-dev \
    libboost-all-dev autoconf-archive libcap-dev gperf wget unzip libuv1-dev \
    libtool \
    libharfbuzz-dev \
    libpng-dev \
    libjpeg-dev \
    libtiff-dev \
    zlib1g-dev \
    libfreetype6-dev \
    libxml2-dev \
    libproj-dev \
    libcairo-dev \
    pkg-config \
    libpq-dev \
    libgdal-dev \
    libsqlite3-dev \
    libcurl4-gnutls-dev \
    libwebp-dev

RUN git clone https://github.com/facebook/folly && cd folly && \
    git checkout v2017.05.29.00 && cd folly && autoreconf --install && \
    ./configure && make && make install

RUN git clone https://github.com/facebook/wangle && cd wangle && \
    git checkout v2017.05.29.00 && mkdir build && cd build && \
    cmake ../wangle && make && make install

RUN git clone https://github.com/facebook/proxygen && cd proxygen && \
    git checkout v2017.05.29.00 && cd proxygen && \
    autoreconf -ivf && ./configure && make && make install

RUN git clone --depth 1 https://github.com/datastax/cpp-driver && \
    cd cpp-driver && mkdir build && cd build && cmake .. && \
    make && make install

RUN git clone --depth 1 https://github.com/couchbase/libcouchbase && \
    cd libcouchbase && mkdir build && cd build && ../cmake/configure && \
    make && make install

RUN git clone --depth 1 https://github.com/mapbox/variant && cd variant && \
   cp -R include/ /usr/

RUN git clone -b maps_express --depth 1 https://github.com/nanoeti/mapnik && \
    cd mapnik && ./configure PREFIX=/usr && make && make install

RUN git clone https://github.com/nanoeti/mapnik-vector-tile.git  && \
    cd mapnik-vector-tile && make

RUN git clone https://github.com/sputnik-maps/mapbox2mapnik && cd mapbox2mapnik && git checkout c66ffb7 && \
    mkdir build && cd build && cmake -DENABLE_SIMPLE_PLACEMENTS=ON -DCMAKE_BUILD_TYPE=Release .. && \
    make && make install

RUN ldconfig

RUN git clone https://github.com/sputnik-maps/maps-express && \
    mv mapnik-vector-tile/ maps-express/mapnik-vector-tile/ && cd maps-express && \
    git checkout 983d6ff && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make && make install

RUN rm -rf /tmp/*

WORKDIR /opt/sputnik/maps/maps-express/

ENTRYPOINT /opt/sputnik/maps/maps-express/maps-express ${HostIP}:8082 etcd ${EtcdHost}
