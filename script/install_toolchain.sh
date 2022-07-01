#!/bin/bash

unamestr=$(uname)

if [[ "$unamestr" == 'Linux' ]]; then
    if [ -f /etc/lsb-release ] && cat /etc/lsb-release | grep DISTRIB_ID | grep Ubuntu >/dev/null; then
        echo "Installing toolchain..."
        if apt-get update >/dev/null; then
            if [[ "$(lsb_release -sr)" == "22.04" ]]; then
                # arrow requires an apt source
                wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
                apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
                apt-get update
                # tzdata requires preconfigured, noninteractive installation
                echo "tzdata tzdata/Areas select Europe" | debconf-set-selections
                echo "tzdata tzdata/Zones/Europe select Berlin" | debconf-set-selections
                DEBIAN_FRONTEND=noninteractive apt-get install -y \
                ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
                binutils-dev \
                ca-certificates \
                ccache \
                clang-14 \
                clang-format-14 \
                clang-tidy-14 \
                cmake \
                cppcheck \
                curl \
                g++-11 \
                gcc-11 \
                gdb \
                git \
                libarrow-dev \
                libarrow-glib-dev \
                libarrow-dataset-dev \
                libarrow-dataset-glib-dev \
                libboost-all-dev \
                libcurl4-openssl-dev \
                libparquet-dev \
                libparquet-glib-dev \
                libssl-dev \
                make \
                ninja-build \
                python3 \
                python3-pip \
                tzdata \
                uuid-dev \
                wget \
                zip \
                zlib1g-dev

                sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-14 140 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-14
                sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-14 140
                sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-14 140
                sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 112 --slave /usr/bin/g++ g++ /usr/bin/g++-11

                pip3 install --no-input --quiet \
                boto3 \
                cpplint \
                ipympl \
                ipywidgets \
                jupyterlab \
                matplotlib \
                pandas \
                pytictoc \
                termcolor \
                tqdm \
                yapf

                # Symlink to default Amazon Linux certificate file
                sudo mkdir -p /etc/pki/tls/certs
                sudo ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
            else
                echo "Error: Ubuntu version must be 22.04"
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
