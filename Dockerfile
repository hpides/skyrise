# Tool versions and locations
ARG CCACHE_VERSION=4.1
ARG CCACHE_DIR=/opt/ccache-${CCACHE_VERSION}
ARG CMAKE_VERSION=3.18
ARG CMAKE_PATCH=5
ARG CMAKE_DIR=/opt/cmake-${CMAKE_VERSION}.${CMAKE_PATCH}
ARG CPPCHECK_VERSION=2.2
ARG CPPCHECK_DIR=/opt/cppcheck-${CPPCHECK_VERSION}
ARG CPPLINT_COMMIT=d5b5104
ARG CPPLINT_DIR=/opt/cpplint-${CPPLINT_COMMIT}
ARG DOCKER_LAMBDA_COMMIT=4e1b563
ARG DOCKER_LAMBDA_DIR=/opt/docker-lambda-${DOCKER_LAMBDA_COMMIT}
ARG GCC_VERSION=7.5.0
ARG GCC_SUFFIX=75
ARG GCC_DIR=/opt/gcc-${GCC_VERSION}
ARG LLVM_VERSION=11.0.0
ARG LLVM_DIR=/opt/llvm-${LLVM_VERSION}


# Packages
FROM lambci/lambda-base-2:build AS base-install

RUN yum install -y \
    # General
    wget \
    # Ccache dependency
    libzstd-devel \
    # Lambda bootstrap wrapper dependency
    golang.x86_64 \
    # LLDB dependency
    libedit-devel && \
    # Cleanup
    yum remove -y \
    cmake && \
    yum clean all && \
    rm -rf /var/cache/yum

# CMake
FROM base-install AS base-cmake
ARG CMAKE_VERSION
ARG CMAKE_PATCH
ARG CMAKE_DIR

WORKDIR ${CMAKE_DIR}
RUN wget -nv https://cmake.org/files/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.${CMAKE_PATCH}-Linux-x86_64.tar.gz -O - \
        | tar -xz --strip-components=1 && \
    for file in /opt/*/bin/*; \
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
    cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${CCACHE_DIR} && \
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
    cmake .. -DCMAKE_INSTALL_PREFIX=${CPPCHECK_DIR} -DFILESDIR=${CPPCHECK_DIR}/share && \
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


# Lambda bootstrap wrapper (init.go)
FROM base-install AS base-docker-lambda
ARG DOCKER_LAMBDA_DIR
ARG DOCKER_LAMBDA_COMMIT

WORKDIR ${DOCKER_LAMBDA_DIR}/src
RUN wget -nv https://raw.githubusercontent.com/lambci/docker-lambda/${DOCKER_LAMBDA_COMMIT}/provided/run/go.mod && \
    wget -nv https://raw.githubusercontent.com/lambci/docker-lambda/${DOCKER_LAMBDA_COMMIT}/provided/run/go.sum && \
    wget -nv https://raw.githubusercontent.com/lambci/docker-lambda/${DOCKER_LAMBDA_COMMIT}/provided/run/init.go && \
    go mod download && \
    GOARCH=amd64 GOOS=linux go build init.go && \
    mv ${DOCKER_LAMBDA_DIR}/src/init ${DOCKER_LAMBDA_DIR}/init && \
    rm -rf ${DOCKER_LAMBDA_DIR}/src


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


# LLVM & Clang
FROM base-cmake AS base-llvm-clang
ARG LLVM_VERSION
ARG LLVM_DIR

WORKDIR ${LLVM_DIR}/src
RUN wget -nv https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/llvm-project-${LLVM_VERSION}.tar.xz -O - \
        | tar -xJ --strip-components=1 && \
    mkdir build && \
    cd build && \
    cmake ../llvm -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${LLVM_DIR} \
        -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;compiler-rt;lld;lldb" && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${LLVM_DIR}/src


# Base stage combining all tools
FROM lambci/lambda-base-2:build AS base
ARG CCACHE_DIR
ARG CMAKE_DIR
ARG CPPCHECK_DIR
ARG CPPLINT_DIR
ARG DOCKER_LAMBDA_DIR
ARG GCC_DIR
ARG LLVM_DIR

COPY --from=base-ccache ${CCACHE_DIR} ${CCACHE_DIR}
COPY --from=base-cmake ${CMAKE_DIR} ${CMAKE_DIR}
COPY --from=base-cppcheck ${CPPCHECK_DIR} ${CPPCHECK_DIR}
COPY --from=base-cpplint ${CPPLINT_DIR} ${CPPLINT_DIR}
COPY --from=base-docker-lambda ${DOCKER_LAMBDA_DIR} ${DOCKER_LAMBDA_DIR}
COPY --from=base-gcc ${GCC_DIR} ${GCC_DIR}
COPY --from=base-llvm-clang ${LLVM_DIR} ${LLVM_DIR}


# Build stage
FROM lambci/lambda-base-2:build AS build
ARG GCC_DIR

# Packages
RUN yum install -y \
    # Build system
    ninja-build \
    # Ccache dependency
    libzstd-devel \
    # Stack traces
    binutils-devel \
    # AWS SDK dependency
    libcurl-devel \
    libuuid-devel \
    openssl-devel \
    system-lsb-core && \
    # Cleanup
    yum remove -y \
    clang \
    cmake \
    llvm && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    # Default commands
    alternatives --install /usr/bin/ld ld /usr/bin/ld.lld 1000 && \
    alternatives --set ld /usr/bin/ld.lld

COPY --from=base /opt /opt

RUN for file in /opt/*/bin/*; \
    do \
        ln -s $file /usr/bin/$(basename $file); \
    done && \
    cp -r ${GCC_DIR}/{lib,lib64,include} /usr && \
    mv /usr/bin/ccache /usr/local/bin/ccache && \
    ln -s /usr/local/bin/ccache /usr/local/bin/clang && \
    ln -s /usr/local/bin/ccache /usr/local/bin/clang++

ENV CC=clang \
    CXX=clang++

# Run stage
FROM lambci/lambda-base-2 AS run
ARG DOCKER_LAMBDA_DIR
ARG LLVM_DIR

# Copy over LLDB
COPY --from=base-llvm-clang ${LLVM_DIR}/bin/lldb* ${LLVM_DIR}/bin/
COPY --from=base-llvm-clang ${LLVM_DIR}/include/lldb ${LLVM_DIR}/include/lldb/
COPY --from=base-llvm-clang ${LLVM_DIR}/lib/liblldb* ${LLVM_DIR}/lib/
COPY --from=base-llvm-clang /usr/lib64/ /usr/lib64/
RUN for file in ${LLVM_DIR}/bin/*; \
        do \
            ln -s $file /usr/bin/$(basename $file); \
        done
# In the AWS Lambda execution environment, language runtimes are located in /var/runtime
WORKDIR /var/runtime/
COPY script/docker/local_function_bootstrap.sh bootstrap
COPY --from=base-docker-lambda ${DOCKER_LAMBDA_DIR}/init bootstrap_wrapper
RUN chmod +x /var/runtime/bootstrap && \
    chmod +x /var/runtime/bootstrap_wrapper
ENV PATH=/var/lang/bin:$PATH \
    LD_LIBRARY_PATH=/var/lang/lib:$LD_LIBRARY_PATH
ENTRYPOINT ["/var/runtime/bootstrap_wrapper"]

FROM ubuntu:20.10 AS ubuntu
RUN apt-get update && \
    apt-get install -y \
    lsb-release \
    ninja-build \
    sudo \
    tzdata
COPY script/install_toolchain.sh install_toolchain.sh
RUN ./install_toolchain.sh && \
    rm install_toolchain.sh
