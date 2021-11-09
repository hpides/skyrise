# Tool versions
ARG AWS_SDK_VERSION=1.9.133
ARG BOOST_VERSION=1.77.0
ARG CCACHE_VERSION=4.4.2
ARG CMAKE_MAJOR_MINOR=3.21
ARG CMAKE_PATCH=4
ARG CPPCHECK_VERSION=2.6
ARG CPPLINT_COMMIT=9806df8
ARG GCC_VERSION=7.5.0
ARG GCC_SUFFIX=75
ARG HEAPTRACK_VERSION=1.2.0
ARG LLVM_CLANG_VERSION=13.0.0
ARG ORC_VERSION=1.7.0
ARG VALGRIND_VERSION=3.18.1

# Tool locations
ARG AWS_SDK_DIR=/opt/build/aws-sdk-${AWS_SDK_VERSION}
ARG BOOST_DIR=/opt/build/boost-${BOOST_VERSION}
ARG CCACHE_DIR=/opt/build/ccache-${CCACHE_VERSION}
ARG CMAKE_DIR=/opt/build/cmake-${CMAKE_MAJOR_MINOR}.${CMAKE_PATCH}
ARG CPPCHECK_DIR=/opt/build/cppcheck-${CPPCHECK_VERSION}
ARG CPPLINT_DIR=/opt/build/cpplint-${CPPLINT_COMMIT}
ARG GCC_DIR=/opt/build/gcc-${GCC_VERSION}
ARG HEAPTRACK_DIR=/opt/run/heaptrack-${HEAPTRACK_VERSION}
ARG LLVM_CLANG_DIR=/opt/build/llvm-${LLVM_CLANG_VERSION}
ARG ORC_DIR=/opt/build/orc-${ORC_VERSION}
ARG VALGRIND_DIR=/opt/run/valgrind-${VALGRIND_VERSION}


# Packages for builing Docker images
FROM amazon/aws-sam-cli-build-image-provided.al2 AS base-install

    # Update packages
RUN yum update -y && \
    # Install packages
    yum install -y \
    # General
    wget \
    # AWS SDK dependency
    libcurl-devel \
    libuuid-devel \
    openssl-devel \
    # Boost dependency \
    python-devel \
    which \
    # Ccache dependency
    libzstd-devel \
    # Heaptrack dependency
    boost-devel \
    libdwarf-devel \
    libunwind-devel \
    # LLDB dependency
    libedit-devel && \
    # Cleanup packages
    yum remove -y \
    cmake && \
    yum clean all && \
    rm -rf /var/cache/yum

# CMake
FROM base-install AS base-cmake
ARG CMAKE_MAJOR_MINOR
ARG CMAKE_PATCH
ARG CMAKE_DIR

WORKDIR ${CMAKE_DIR}
RUN wget -nv https://cmake.org/files/v${CMAKE_MAJOR_MINOR}/cmake-${CMAKE_MAJOR_MINOR}.${CMAKE_PATCH}-linux-x86_64.tar.gz -O - \
        | tar -xz --strip-components=1 && \
    for file in ${CMAKE_DIR}/bin/*; \
        do \
            ln -s $file /usr/bin/$(basename $file); \
        done


# Ccache
FROM base-cmake AS base-ccache
ARG CCACHE_VERSION
ARG CCACHE_DIR

WORKDIR ${CCACHE_DIR}/src
RUN wget -nv https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}.tar.gz -O - \
        | tar -xz --strip-components=1 && \
    mkdir build && \
    cd build && \
    cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=${CCACHE_DIR} \
            -DREDIS_STORAGE_BACKEND=OFF && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${CCACHE_DIR}/src


# Cppcheck
FROM base-cmake AS base-cppcheck
ARG CPPCHECK_VERSION
ARG CPPCHECK_DIR

WORKDIR ${CPPCHECK_DIR}/src
RUN wget -nv https://github.com/danmar/cppcheck/archive/${CPPCHECK_VERSION}.tar.gz -O - \
        | tar -xz --strip-components=1 && \
    mkdir build && \
    cd build && \
    cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=${CPPCHECK_DIR} \
            -DFILESDIR=${CPPCHECK_DIR}/share && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${CPPCHECK_DIR}/src


# Cpplint
FROM base-install AS base-cpplint
ARG CPPLINT_COMMIT
ARG CPPLINT_DIR

WORKDIR ${CPPLINT_DIR}/bin
RUN wget -nv  https://raw.githubusercontent.com/google/styleguide/${CPPLINT_COMMIT}/cpplint/cpplint.py && \
    chmod +x cpplint.py


# GCC
FROM base-install AS base-gcc
ARG GCC_VERSION
ARG GCC_SUFFIX
ARG GCC_DIR

WORKDIR ${GCC_DIR}/src
RUN wget -nv https://mirrors.kernel.org/gnu/gcc/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.gz -O - \
        | tar -xz --strip-components=1 && \
    ./contrib/download_prerequisites && \
    mkdir build && \
    cd build && \
    ../configure --enable-languages=c,c++ --disable-multilib --prefix=${GCC_DIR} --program-suffix=${GCC_SUFFIX} && \
    make -j$(nproc) && \
    make install-strip && \
    rm -rf ${GCC_DIR}/src


# Heaptrack
FROM base-cmake AS base-heaptrack
ARG HEAPTRACK_VERSION
ARG HEAPTRACK_DIR

WORKDIR ${HEAPTRACK_DIR}/src
RUN wget -nv https://github.com/KDE/heaptrack/archive/v${HEAPTRACK_VERSION}.tar.gz -O - \
        | tar -xz --strip-components=1 && \
    mkdir build && \
    cd build && \
    cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=${HEAPTRACK_DIR} && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${HEAPTRACK_DIR}/src


# LLVM & Clang
FROM base-cmake AS base-llvm-clang
ARG LLVM_CLANG_VERSION
ARG LLVM_CLANG_DIR

WORKDIR ${LLVM_CLANG_DIR}/src
RUN wget -nv https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_CLANG_VERSION}/llvm-project-${LLVM_CLANG_VERSION}.src.tar.xz -O - \
        | tar -xJ --strip-components=1 && \
    mkdir build && \
    cd build && \
    cmake ../llvm \
                 -DCMAKE_BUILD_TYPE=Release \
                 -DCMAKE_INSTALL_PREFIX=${LLVM_CLANG_DIR} \
                 -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;compiler-rt;lld;lldb" && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${LLVM_CLANG_DIR}/src && \
    for file in ${LLVM_CLANG_DIR}/bin/*; \
        do \
            ln -s $file /usr/bin/$(basename $file); \
        done
ENV CC=clang \
    CXX=clang++


# AWS SDK
FROM base-llvm-clang AS base-aws-sdk
ARG AWS_SDK_VERSION
ARG AWS_SDK_DIR

WORKDIR ${AWS_SDK_DIR}
RUN git clone --branch ${AWS_SDK_VERSION} --depth 1 --recurse-submodules --shallow-submodules https://github.com/aws/aws-sdk-cpp.git src && \
    mkdir -p src/build && \
    cd src/build && \
    cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=${AWS_SDK_DIR} \
            -DBUILD_ONLY="ec2;iam;lambda;monitoring;pricing;s3;sqs;xray" \
            -DBUILD_SHARED_LIBS=OFF \
            -DCPP_STANDARD=17 \
            -DCUSTOM_MEMORY_MANAGEMENT=OFF \
            -DENABLE_TESTING=OFF \
            -DMINIMIZE_SIZE=ON \
            -DTARGET_ARCH=LINUX && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${AWS_SDK_DIR}/src


# Boost
FROM base-llvm-clang AS base-boost
ARG BOOST_VERSION
ARG BOOST_DIR

WORKDIR ${BOOST_DIR}
RUN git clone --branch boost-${BOOST_VERSION} --depth 1 --recurse-submodules --shallow-submodules https://github.com/boostorg/boost src && \
    cd src && \
    ./bootstrap.sh --prefix=${BOOST_DIR} --with-toolset=clang && \
    ./b2 \
        toolset=clang \
        variant=release \
        link=static \
        cxxflags="-std=c++17" \
        -j$(nproc) \
        --with-math \
        --with-serialization \
        --with-stacktrace \
        install && \
    rm -rf ${BOOST_DIR}/src


# Valgrind
FROM base-install AS base-valgrind
ARG VALGRIND_VERSION
ARG VALGRIND_DIR

WORKDIR ${VALGRIND_DIR}/src
RUN wget -nv https://sourceware.org/pub/valgrind/valgrind-${VALGRIND_VERSION}.tar.bz2 -O - \
    | tar -xj --strip-components=1 && \
    ./autogen.sh  && \
    ./configure --prefix=${VALGRIND_DIR} && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${VALGRIND_DIR}/src


# Base stage combining all tools
FROM amazon/aws-sam-cli-build-image-provided.al2 AS base
ARG AWS_SDK_DIR
ARG BOOST_DIR
ARG CCACHE_DIR
ARG CMAKE_DIR
ARG CPPCHECK_DIR
ARG CPPLINT_DIR
ARG GCC_DIR
ARG HEAPTRACK_DIR
ARG LLVM_CLANG_DIR
ARG VALGRIND_DIR

COPY --from=base-aws-sdk ${AWS_SDK_DIR} ${AWS_SDK_DIR}
COPY --from=base-boost ${BOOST_DIR} ${BOOST_DIR}
COPY --from=base-ccache ${CCACHE_DIR} ${CCACHE_DIR}
COPY --from=base-cmake ${CMAKE_DIR} ${CMAKE_DIR}
COPY --from=base-cppcheck ${CPPCHECK_DIR} ${CPPCHECK_DIR}
COPY --from=base-cpplint ${CPPLINT_DIR} ${CPPLINT_DIR}
COPY --from=base-gcc ${GCC_DIR} ${GCC_DIR}
COPY --from=base-heaptrack ${HEAPTRACK_DIR} ${HEAPTRACK_DIR}
COPY --from=base-llvm-clang ${LLVM_CLANG_DIR} ${LLVM_CLANG_DIR}
COPY --from=base-valgrind ${VALGRIND_DIR} ${VALGRIND_DIR}


# Amazon Linux 2 Docker image for building Skyrise
FROM amazon/aws-sam-cli-build-image-provided.al2 AS amazonlinux2
ARG AWS_SDK_DIR
ARG BOOST_DIR
ARG GCC_DIR

    # Update packages
RUN yum update -y && \
    # Install packages
    yum install -y \
    # AWS SDK dependency
    libcurl-devel \
    libuuid-devel \
    openssl-devel \
    system-lsb-core \
    # Build system
    ninja-build \
    # Ccache dependency
    libzstd-devel \
    # Heaptrack dependency
    libunwind-devel \
    which \
    # Perf
    perf \
    # Stack traces
    binutils-devel && \
    # Cleanup packages
    yum clean all && \
    rm -rf /var/cache/yum && \
    # Python packages
    pip3 install --no-input --quiet \
    pytictoc \
    termcolor \
    yapf && \
    # Default commands
    /usr/sbin/alternatives --install /usr/bin/ld ld /usr/bin/ld.lld 1300 && \
    /usr/sbin/alternatives --set ld /usr/bin/ld.lld

COPY --from=base /opt /opt

RUN for file in /opt/*/*/bin/*; \
    do \
        ln -s $file /usr/bin/$(basename $file); \
    done && \
    cp -r ${AWS_SDK_DIR}/{include,lib64} /usr && \
    cp -r ${BOOST_DIR}/{include,lib} /usr && \
    cp -r ${GCC_DIR}/{include,lib,lib64} /usr && \
    mv /usr/bin/ccache /usr/local/bin/ccache && \
    ln -s /usr/local/bin/ccache /usr/local/bin/clang && \
    ln -s /usr/local/bin/ccache /usr/local/bin/clang++
ENV CC=clang \
    CXX=clang++


# Ubuntu Docker image for building Skyrise
FROM ubuntu:21.10 AS ubuntu
ARG AWS_SDK_VERSION
ARG AWS_SDK_DIR

RUN apt-get update && \
    apt-get install -y \
    lsb-release \
    sudo

# Install packages
COPY script/install_toolchain.sh install_toolchain.sh

RUN ./install_toolchain.sh && \
    rm install_toolchain.sh

# Build and install AWS SDK
WORKDIR ${AWS_SDK_DIR}
RUN git clone --branch ${AWS_SDK_VERSION} --depth 1 --recurse-submodules --shallow-submodules https://github.com/aws/aws-sdk-cpp.git src && \
    mkdir -p src/build && \
    cd src/build && \
    cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_CXX_FLAGS="-Wno-error=nonnull" \
            -DBUILD_ONLY="ec2;iam;lambda;monitoring;pricing;s3;sqs;xray" \
            -DBUILD_SHARED_LIBS=OFF \
            -DCPP_STANDARD=17 \
            -DCUSTOM_MEMORY_MANAGEMENT=OFF \
            -DENABLE_TESTING=OFF \
            -DMINIMIZE_SIZE=ON \
            -DTARGET_ARCH=LINUX && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${AWS_SDK_DIR}
