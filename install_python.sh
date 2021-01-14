#!/bin/bash

sudo yum -y install gcc openssl-devel bzip2-devel
cd /tmp/
wget https://www.python.org/ftp/python/3.6.6/Python-3.6.6.tgz
tar xzf Python-3.6.6.tgz
rm -rf Python-3.6.6.tgz
cd Python-3.6.6
./configure --enable-optimizations
sudo make altinstall

sudo ln -sfn /usr/local/bin/python3.6 /usr/bin/python3.6
sudo yum install cyrus-sasl-devel
