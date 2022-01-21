FROM euleros:x86-V200R007C00SPC522B030
MAINTAINER l50021422

WORKDIR /tmp/
COPY * /tmp/

RUN chmod 777 /tmp
RUN /bin/bash pre_install.sh
#RUN rm -rf /tmp/*

RUN sed -i "s/^auth.*required/#&/" /etc/pam.d/su \
&& echo "root:Changeme_123" | chpasswd \
&& echo "dbuser:Changeme_123" | chpasswd

# install GaussDB-Kernel
WORKDIR /tmp/
RUN mkdir /tmp/GaussDB-Kernel-V500R002C00-EULER-64bit && tar zxvf GaussDB-Kernel-V500R002C00-EULER-64bit.tar.gz -C /tmp/GaussDB-Kernel-V500R002C00-EULER-64bit/ && chown -R dbuser:dbgroup /tmp/GaussDB-Kernel-V500R002C00-EULER-64bit

USER dbuser:dbgroup
RUN whoami && source /home/dbuser/.bashrc && /bin/bash /tmp/GaussDB-Kernel-V500R002C00-EULER-64bit/install.sh --mode single -D /opt/data1/greenopengauss/data/0 -R /opt/data1/greenopengauss/app -P "-w Changeme_123" -P "-E UTF-8" -P "--locale=zh_CN.UTF-8"
RUN source /home/dbuser/.bashrc

USER root
# RUN /bin/bash post_install.sh
RUN rm -rf /opt/data1/greenopengauss/data/0
RUN rm -rf /opt/data1/greenopengauss/pre
RUN rm -rf /tmp/

COPY config /opt/data1/greenopengauss/
RUN chown dbuser:dbgroup /opt/data1/greenopengauss/config

COPY gs_probackup /opt/data1/greenopengauss/app/bin/
RUN chown dbuser:dbgroup /opt/data1/greenopengauss/app/bin/gs_probackup

USER dbuser:dbgroup
WORKDIR /opt/data1/greenopengauss/

ENTRYPOINT ["/bin/bash"]
# CMD [""]


