#!/bin/bash

unamestr=$(uname)

if [[ "$unamestr" == 'Linux' ]]; then
    if [ -f /etc/lsb-release ] && cat /etc/lsb-release | grep DISTRIB_ID | grep Ubuntu >/dev/null; then
        echo "Installing toolchain..."   
        if sudo apt-get update >/dev/null; then
            if [[ "$(lsb_release -sr)" == "21.04" ]]; then
                sudo apt-get install --no-install-recommends -y \
                binutils-dev \
                ca-certificates \
                ccache \
                clang-12 \
                clang-format-12 \
                clang-tidy-12 \
                cmake \
                cppcheck \
                curl \
                g++-10 \
                gcc-10 \
                git \
                libcurl4-openssl-dev \
                libssl-dev \
                make \
                python3 \
                python3-pip \
                uuid-dev \
                zip \
                zlib1g-dev
                
                sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-12 120 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-12
                sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-12 120
                sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-12 120
                sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 103 --slave /usr/bin/g++ g++ /usr/bin/g++-10

                pip3 install --no-input --quiet \
                cpplint \
                pytictoc \
                termcolor \
                yapf

                # Symlink to default Amazon Linux certificate file
                sudo mkdir -p /etc/pki/tls/certs
                sudo ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
            else
                echo "Error: Ubuntu version must be 20.04"
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
