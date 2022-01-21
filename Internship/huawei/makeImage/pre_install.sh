#!/bin/bash
current_path=$(cd $(dirname $0);pwd)
gauss_root=/opt/data1/greenopengauss
pkg_path=$current_path/GaussDB-Kernel-V500R002C00-EULER-64bit

cp $current_path/EulerOS_sp5.repo /etc/yum.repos.d/EulerOS_sp5.repo

# install requirements
yum update -y 
yum install -y vim tar wget passwd.x86_64 gcc gcc-c++ make libffi* zlib* libxml2* libxslt sqlite lsof.x86_64 net-tools.x86_64


# install openssl
cd $current_path
tar zxvf openssl-1.1.1g.tar.gz && cd openssl-1.1.1g
mkdir /usr/local/openssl && ./config shared --prefix=/usr/local/openssl
make && make install
ln -s /usr/local/openssl/bin/openssl /usr/bin/openssl
ln -s /usr/local/openssl/include/openssl /usr/include/openssl
echo "/usr/local/openssl/lib" >> /etc/ld.so.conf && ldconfig


# install python3
cd $current_path
tar zxvf $current_path/Python-3.8.9.tgz && cd Python-3.8.9
mkdir /usr/local/python3 && ./configure --prefix=/usr/local/python3 --with-openssl=/usr/local/openssl
make && make install
ln -s /usr/local/python3/bin/python3 /usr/bin/python3
ln -s /usr/local/python3/bin/pip3 /usr/bin/pip3

# install client-python
mkdir ~/.pip
cat << EOF > ~/.pip/pip.conf
[global]
trusted-host=cmc-cd-mirror.rnd.huawei.com
index-url=http://cmc-cd-mirror.rnd.huawei.com/pypi/simple/
EOF
python3 -m pip install kubernetes
chmod -R 755 /usr/local/python3 /usr/local/openssl


# add psycopg2
cd $current_path
mkdir -p $gauss_root/ && tar zxvf green.tgz -C $gauss_root/
cp $current_path/*.py $gauss_root/


# add dbuser && set env
groupadd -g 1999 dbgroup && groupadd -g 2000 ossgroup
useradd -u 3002 -g dbgroup -G ossgroup -d /home/dbuser -m -p Changeme_123 -s /bin/bash dbuser

chown -R dbuser:dbgroup /opt/data1
cat << EOF >> /home/dbuser/.bashrc
export GAUSSHOME=$gauss_root/pre
export PATH=\$GAUSSHOME/bin:\$PATH
export LD_LIBRARY_PATH=\$GAUSSHOME/lib:\$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$gauss_root/lib:\$LD_LIBRARY_PATH
EOF

# install GaussDB-Kernel
#cd $current_path
#mkdir $pkg_path && tar zxvf GaussDB-Kernel-V500R002C00-EULER-64bit.tar.gz -C $pkg_path/ && chown -R dbuser:dbgroup $pkg_path

#sudo -u dbsuer /bin/bash -c "source /home/dbuser/.bashrc && /bin/bash $pkg_path/install.sh --mode single -D /opt/data1/greenopengauss/data/0 -R /opt/data1/greenopengauss/app -P \"-w Changeme_123\" -P \"-E UTF-8\" -P \"--locale=zh_CN.UTF-8\" && source /home/dbuser/.bashrc"

#/bin/bash post_install.sh
#rm -rf /opt/data1/greenopengauss/data/0
#rm -rf /opt/data1/greenopengauss/pre
#rm -rf /tmp/

