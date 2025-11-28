# Installation python 3.11 et Airbyte

sudo apt update
sudo apt install -y build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses-dev libffi-dev liblzma-dev python3-openssl

cd /tmp
wget https://www.python.org/ftp/python/3.11.4/Python-3.11.4.tgz

tar -xvf Python-3.11.4.tgz
cd Python-3.11.4
sudo apt install pkg-config
./configure --enable-optimizations --prefix=/usr/local
make -j $(nproc)
sudo make altinstall

Dans le .venv :
pip install airbyte
