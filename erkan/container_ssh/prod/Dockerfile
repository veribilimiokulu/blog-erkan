FROM rockylinux:8.6

ENV container docker

RUN yum -y update && yum clean all

RUN yum -y install openssh-server  python3 sudo passwd  systemd

RUN adduser -m prod_user &&     echo "Ankara06" | passwd prod_user --stdin &&     usermod -aG wheel prod_user &&     mkdir /home/prod_user/.ssh &&     chown prod_user:prod_user -R  /home/prod_user/ &&     chmod 700 /home/prod_user/.ssh

RUN     echo "%wheel         ALL = (ALL) NOPASSWD: ALL" >> /etc/sudoers

RUN echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config

RUN systemctl enable sshd.service

RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [  == systemd-tmpfiles-setup.service ] || rm -f ; done); rm -f /lib/systemd/system/multi-user.target.wants/*;rm -f /etc/systemd/system/*.wants/*;rm -f /lib/systemd/system/local-fs.target.wants/*; rm -f /lib/systemd/system/sockets.target.wants/*udev*; rm -f /lib/systemd/system/sockets.target.wants/*initctl*; rm -f /lib/systemd/system/basic.target.wants/*;rm -f /lib/systemd/system/anaconda.target.wants/*;

COPY init_script.sh /etc/init.d/

VOLUME [ "/sys/fs/cgroup" ]

CMD ["/usr/sbin/init"]
