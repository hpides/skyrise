#!/bin/bash

unamestr=$(uname)

if [[ "$unamestr" == 'Linux' ]]; then
    if [ -f /etc/lsb-release ] && cat /etc/lsb-release | grep DISTRIB_ID | grep Ubuntu >/dev/null; then
        echo "Installing toolchain..."   
        if sudo apt-get update >/dev/null; then
            if [[ "$(lsb_release -sr)" == "21.10" ]]; then
                sudo apt-get install --no-install-recommends -y \
                binutils-dev \
                ca-certificates \
                ccache \
                clang-13 \
                clang-format-13 \
                clang-tidy-13 \
                cmake \
                cppcheck \
                curl \
                g++-11 \
                gcc-11 \
                git \
                libboost-all-dev \
                libcurl4-openssl-dev \
                libssl-dev \
                make \
                ninja-build \
                python3 \
                python3-pip \
                uuid-dev \
                wget \
                zip \
                zlib1g-dev
                
                sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-13 130 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-13
                sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-13 130
                sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-13 130
                sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 112 --slave /usr/bin/g++ g++ /usr/bin/g++-11

                pip3 install --no-input --quiet \
                cpplint \
                pytictoc \
                termcolor \
                yapf

                # Symlink to default Amazon Linux certificate file
                sudo mkdir -p /etc/pki/tls/certs
                sudo ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
            else
                echo "Error: Ubuntu version must be 21.10"
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
