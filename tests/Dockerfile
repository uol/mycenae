FROM centos:7

ENV GOPATH=/var/go/
ENV GOROOT=/usr/lib/golang/

RUN yum -y install epel-release && \
    yum -y install golang && \
	mkdir -p /var/go/ && \
	rm -f /etc/localtime && \
	ln -s '/usr/share/zoneinfo/America/Sao_Paulo' /etc/localtime