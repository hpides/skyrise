# Tool versions
ARG ARROW_VERSION=18.1.0
ARG AWS_SDK_VERSION=1.11.462
ARG BACKTRACE_VERSION=1db8564
ARG BOOST_VERSION=1.86.0
ARG CCACHE_VERSION=4.10.2
ARG CMAKE_MAJOR_MINOR=3.31
ARG CMAKE_PATCH=2
ARG CPPLINT_COMMIT=8f97e24
ARG HEAPTRACK_VERSION=1.3.0
ARG IPERF_VERSION=3.17.1
ARG SHELLCHECK_VERSION=0.10.0

# Tool locations
ARG ARROW_DIR=/opt/build/arrow-${ARROW_VERSION}
ARG AWS_SDK_DIR=/opt/build/aws-sdk-${AWS_SDK_VERSION}
ARG BACKTRACE_DIR=/opt/build/backtrace-${BACKTRACE_VERSION}
ARG BOOST_DIR=/opt/build/boost-${BOOST_VERSION}
ARG CCACHE_DIR=/opt/build/ccache-${CCACHE_VERSION}
ARG CMAKE_DIR=/opt/build/cmake-${CMAKE_MAJOR_MINOR}.${CMAKE_PATCH}
ARG CPPLINT_DIR=/opt/build/cpplint-${CPPLINT_COMMIT}
ARG HEAPTRACK_DIR=/opt/run/heaptrack-${HEAPTRACK_VERSION}
ARG IPERF_DIR=/opt/build/iperf-${IPERF_VERSION}
ARG SHELLCHECK_DIR=/opt/build/shellcheck-${SHELLCHECK_VERSION}

# The Docker images are based on the latest Amazon Linux 2023 (AL2023) Serverless Application Model (SAM) image.
# We set different environment variables based on the target processor architecture.
FROM public.ecr.aws/sam/build-provided.al2023:latest AS base-install-amd64
ENV CMAKE_ARCH=x86_64


FROM public.ecr.aws/sam/build-provided.al2023:latest AS base-install-arm64
ENV CMAKE_ARCH=aarch64


# Packages for building Docker images
FROM base-install-${TARGETARCH} AS base-install
ARG TARGETARCH

    # Update packages
RUN dnf update -y && \
    # Install packages
    dnf install -y \
    # General
    wget \
    xz \
    # AWS SDK dependency
    libcurl-devel \
    openssl-devel \
    # Boost
    boost-devel \
    # Heaptrack dependency
    libdwarf-devel \
    libunwind-devel \
    # LLVM & Clang
    clang-devel && \
    # Cleanup packages
    dnf clean all && \
    rm -rf /var/cache/yum


# Backtrace
FROM base-install AS base-backtrace
ARG BACKTRACE_VERSION
ARG BACKTRACE_DIR

WORKDIR ${BACKTRACE_DIR}
RUN git clone https://github.com/ianlancetaylor/libbacktrace.git src && \
    cd src && \
    git checkout ${BACKTRACE_VERSION} && \
    ./configure --prefix=${BACKTRACE_DIR} && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${BACKTRACE_DIR}/src


# Boost
FROM base-install AS base-boost
ARG BACKTRACE_DIR
ARG BOOST_VERSION
ARG BOOST_DIR

COPY --from=base-backtrace ${BACKTRACE_DIR} ${BACKTRACE_DIR}
RUN cp -r ${BACKTRACE_DIR}/{include,lib} /usr

WORKDIR ${BOOST_DIR}
RUN git clone --branch boost-${BOOST_VERSION} --depth 1 --recurse-submodules --shallow-submodules https://github.com/boostorg/boost src && \
    cd src && \
    ./bootstrap.sh --prefix=${BOOST_DIR} --with-toolset=clang && \
    ./b2 \
        toolset=clang \
        variant=release \
        link=static \
        cxxflags="-std=c++20" \
        -j$(nproc) \
        --with-math \
        --with-serialization \
        --with-stacktrace \
        --with-system \
        install && \
    rm -rf ${BOOST_DIR}/src


# CMake
FROM base-install AS base-cmake
ARG CMAKE_MAJOR_MINOR
ARG CMAKE_PATCH
ARG CMAKE_DIR
ARG CMAKE_ARCH

WORKDIR ${CMAKE_DIR}
RUN wget -nv https://cmake.org/files/v${CMAKE_MAJOR_MINOR}/cmake-${CMAKE_MAJOR_MINOR}.${CMAKE_PATCH}-linux-${CMAKE_ARCH}.tar.gz -O - \
        | tar -xz --strip-components=1 && \
    for file in ${CMAKE_DIR}/bin/*; \
        do \
            ln -s $file /usr/bin/$(basename $file); \
        done


# AWS SDK
FROM base-cmake AS base-aws-sdk
ARG AWS_SDK_VERSION
ARG AWS_SDK_DIR

WORKDIR ${AWS_SDK_DIR}
RUN git clone --branch ${AWS_SDK_VERSION} --depth 1 --recurse-submodules --shallow-submodules https://github.com/aws/aws-sdk-cpp.git src && \
    mkdir -p src/build && \
    cd src/build && \
    cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=${AWS_SDK_DIR} \
            -DBUILD_ONLY="dynamodb;ec2;ecr;elasticfilesystem;events;glue;iam;lambda;logs;monitoring;pricing;s3;sqs;ssm;xray" \
            -DBUILD_SHARED_LIBS=OFF \
            -DCPP_STANDARD=20 \
            -DENABLE_TESTING=OFF \
            -DTARGET_ARCH=LINUX && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${AWS_SDK_DIR}/src


# Arrow
FROM base-cmake AS base-arrow
ARG ARROW_VERSION
ARG ARROW_DIR
ARG AWS_SDK_DIR

COPY --from=base-aws-sdk ${AWS_SDK_DIR} ${AWS_SDK_DIR}
RUN cp -r ${AWS_SDK_DIR}/{include,lib64} /usr

WORKDIR ${ARROW_DIR}/src
RUN wget -nv https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_VERSION}.tar.gz -O - \
    | tar -xz --strip-components=1 && \
    mkdir -p cpp/build && \
    cd cpp/build && \
    cmake .. \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=${ARROW_DIR} \
            -DARROW_BUILD_SHARED=OFF \
            -DARROW_BUILD_STATIC=ON \
            -DARROW_COMPUTE=ON \
            -DARROW_DATASET=ON \
            -DARROW_DEPENDENCY_SOURCE=AUTO \
            -DARROW_DEPENDENCY_USE_SHARED=OFF \
            -DARROW_FILESYSTEM=ON \
            -DARROW_ORC=ON \
            -DARROW_PARQUET=ON \
            -DARROW_WITH_BROTLI=ON \
            -DARROW_WITH_BZ2=ON \
            -DARROW_WITH_LZ4=ON \
            -DARROW_WITH_RE2=ON\
            -DARROW_WITH_SNAPPY=ON \
            -DARROW_WITH_ZLIB=ON \
            -DARROW_WITH_ZSTD=ON \
            -DAWSSDK_SOURCE=SYSTEM \
            -DBoost_SOURCE=SYSTEM && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${ARROW_DIR}/src


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


# Cpplint
FROM base-install AS base-cpplint
ARG CPPLINT_COMMIT
ARG CPPLINT_DIR

WORKDIR ${CPPLINT_DIR}/bin
RUN wget -nv  https://raw.githubusercontent.com/google/styleguide/${CPPLINT_COMMIT}/cpplint/cpplint.py && \
    chmod +x cpplint.py


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


# iPerf3
FROM base-install AS base-iperf
ARG IPERF_VERSION
ARG IPERF_DIR

WORKDIR ${IPERF_DIR}
RUN git clone --branch ${IPERF_VERSION} https://github.com/esnet/iperf.git src && \
    cd src && \
    ./configure --prefix=${IPERF_DIR} && \
    make -j$(nproc) && \
    make install && \
    rm -rf ${IPERF_DIR}/src


# ShellCheck
FROM base-install AS base-shellcheck
ARG CMAKE_ARCH
ARG SHELLCHECK_VERSION
ARG SHELLCHECK_DIR

WORKDIR ${SHELLCHECK_DIR}/bin
RUN     wget -nv https://github.com/koalaman/shellcheck/releases/download/v${SHELLCHECK_VERSION}/shellcheck-v${SHELLCHECK_VERSION}.linux.${CMAKE_ARCH}.tar.xz -O - \
            | tar -xJ --strip-components=1


# Base stage combining all tools
FROM public.ecr.aws/sam/build-provided.al2023:latest AS base
ARG ARROW_DIR
ARG AWS_SDK_DIR
ARG BACKTRACE_DIR
ARG BOOST_DIR
ARG CCACHE_DIR
ARG CMAKE_DIR
ARG CPPLINT_DIR
ARG HEAPTRACK_DIR
ARG IPERF_DIR
ARG SHELLCHECK_DIR

COPY --from=base-arrow ${ARROW_DIR} ${ARROW_DIR}
COPY --from=base-aws-sdk ${AWS_SDK_DIR} ${AWS_SDK_DIR}
COPY --from=base-backtrace ${BACKTRACE_DIR} ${BACKTRACE_DIR}
COPY --from=base-boost ${BOOST_DIR} ${BOOST_DIR}
COPY --from=base-ccache ${CCACHE_DIR} ${CCACHE_DIR}
COPY --from=base-cmake ${CMAKE_DIR} ${CMAKE_DIR}
COPY --from=base-cpplint ${CPPLINT_DIR} ${CPPLINT_DIR}
COPY --from=base-heaptrack ${HEAPTRACK_DIR} ${HEAPTRACK_DIR}
COPY --from=base-iperf ${IPERF_DIR} ${IPERF_DIR}
COPY --from=base-shellcheck ${SHELLCHECK_DIR} ${SHELLCHECK_DIR}


# Amazon Linux 2023 Docker image for building Skyrise
FROM public.ecr.aws/sam/build-provided.al2023:latest AS al2023
ARG ARROW_DIR
ARG AWS_SDK_DIR
ARG BACKTRACE_DIR
ARG BOOST_DIR
ARG IPERF_DIR

    # Update packages
RUN dnf update -y && \
    # Install packages
    dnf install -y \
    # AWS SDK dependency
    libcurl-devel \
    openssl-devel \
    # Build system
    ninja-build \
    # Cppcheck
    cppcheck \
    # LLVM & Clang
    clang-devel \
    # LLD \
    lld-devel \
    # LLDB dependency
    libedit-devel \
    # SSH
    libssh-devel \
    # Stack traces
    binutils-devel \
    # Valgrind
    valgrind && \
    # Cleanup packages
    dnf clean all && \
    rm -rf /var/cache/yum && \
    # Python packages
    pip3 install --no-input --quiet \
    boto3 \
    matplotlib \
    numpy \
    pandas \
    pytictoc \
    seaborn \
    termcolor \
    tqdm \
    yapf && \
    # Default commands
    /usr/sbin/alternatives --install /usr/bin/ld ld /usr/bin/ld.lld 1507 && \
    /usr/sbin/alternatives --set ld /usr/bin/ld.lld

COPY --from=base /opt /opt

RUN for file in /opt/*/*/bin/*; \
    do \
        ln -sf $file /usr/bin/$(basename $file); \
    done && \
    cp -r ${ARROW_DIR}/{include,lib64} /usr && \
    cp -r ${AWS_SDK_DIR}/{include,lib64} /usr && \
    cp -r ${BACKTRACE_DIR}/{include,lib} /usr && \
    cp -r ${BOOST_DIR}/{include,lib} /usr && \
    cp -r ${IPERF_DIR}/{include,lib} /usr && \
    mv /usr/bin/ccache /usr/local/bin/ccache && \
    ln -s /usr/local/bin/ccache /usr/local/bin/clang && \
    ln -s /usr/local/bin/ccache /usr/local/bin/clang++ && \
    ln -s /usr/share/zoneinfo/Europe/Berlin /etc/localtime
ENV CC=clang \
    CXX=clang++
