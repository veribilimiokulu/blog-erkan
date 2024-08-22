FROM jenkins/jenkins:lts-jdk17

USER root

# Install ansible
RUN apt update && apt -y install python3-distutils && apt -y install python3-apt && apt -y install python3 && apt -y install iputils-ping netcat-traditional

RUN apt-get install -y python3-pip

RUN pip install -U ansible --break-system-packages

RUN apt-get -y install rsync

USER jenkins
