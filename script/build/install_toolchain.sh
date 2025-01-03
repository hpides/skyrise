#!/bin/bash

unamestr=$(uname)

if [[ "$unamestr" == 'Linux' ]]; then
    if [ -f /etc/lsb-release ] && grep DISTRIB_ID /etc/lsb-release | grep Ubuntu >/dev/null; then
        echo "Installing toolchain..."
        if apt-get update >/dev/null; then
            if [[ "$(lsb_release -sr)" == "24.04" ]]; then
                # tzdata requires preconfigured, noninteractive installation
                echo "tzdata tzdata/Areas select Europe" | debconf-set-selections
                echo "tzdata tzdata/Zones/Europe select Berlin" | debconf-set-selections
                DEBIAN_FRONTEND=noninteractive apt-get install -y \
                binutils-dev \
                ca-certificates \
                ccache \
                clang-15 \
                clang-format-15 \
                clang-tidy-15 \
                cmake \
                cppcheck \
                curl \
                g++-11 \
                gcc-11 \
                gdb \
                git \
                libboost-all-dev \
                libclang-15-dev \
                libcurl4-openssl-dev \
                libssh-dev \
                libssl-dev \
                llvm-15-dev \
                make \
                ninja-build \
                python3 \
                python3-pip \
                shellcheck \
                terminfo \
                tzdata \
                uuid-dev \
                valgrind \
                wget \
                zip \
                zlib1g-dev

                sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-15 150 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-15
                sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-15 150
                sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-15 150
                sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 114 --slave /usr/bin/g++ g++ /usr/bin/g++-11

                pip3 install --no-input --quiet \
                boto3 \
                cpplint \
                ipympl \
                ipywidgets \
                jupyterlab \
                matplotlib \
                numpy \
                pandas \
                pytictoc \
                seaborn \
                termcolor \
                tqdm \
                yapf

                # Symlink to default Amazon Linux certificate file
                sudo mkdir -p /etc/pki/tls/certs
                sudo ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
            else
                echo "Error: Ubuntu version must be 24.04"
                exit 1
            fi
        else
            echo "Error during installation."
            exit 1
        fi
    else
        echo "Error: OS must be Ubuntu Linux."
        exit 1
    fi
else
    echo "Error: Unsupported operating system $unamestr."
    exit 1
fi

exit 0
