#!/bin/bash

unamestr=$(uname)

if [[ "$unamestr" == 'Linux' ]]; then
    if [ -f /etc/lsb-release ] && cat /etc/lsb-release | grep DISTRIB_ID | grep Ubuntu >/dev/null; then
        echo "Installing toolchain..."   
        if sudo apt-get update >/dev/null; then
            if [[ "$(lsb_release -sr)" == "20.10" ]]; then
                sudo apt-get install --no-install-recommends -y \
                binutils-dev \
                ca-certificates \
                ccache \
                clang-11 \
                clang-format-11 \
                clang-tidy-11 \
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
                pip3 install cpplint
                
                sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-11 110 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-11
                sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-11 110
                sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-11 110
                sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 102 --slave /usr/bin/g++ g++ /usr/bin/g++-10

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
