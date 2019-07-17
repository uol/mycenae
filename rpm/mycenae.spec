#------------------------------------------------------------------------------
# P A C K A G E  I N F O
#------------------------------------------------------------------------------
%define binaryname mycenae
%define projectname mycenae
%define build_timestamp %(date +"%Y%m%d%H%M")
Name:      %{projectname}
Version:   2.18.0
Release:   %{build_timestamp}

Packager:  UOL - Universo Online S.A.
Vendor:    UOL - Universo Online S.A.
URL:       https://www.uol.com.br

Source0:   https://github.com/uol/mycenae
License:   GPLv3

BuildRoot: %{_tmppath}-%{name}-%{version}
BuildArch: x86_64

Summary:   Mycenae timeseries database that uses cassandra and elasticsearch as backends
Group:     Application/Internet

%description
Mycenae is a timeseries database that uses scylla, memcached and elasticsearch as backends.

#------------------------------------------------------------------------------
# B U I L D
#------------------------------------------------------------------------------
%prep

%build
cd ${GOPATH}/src/github.com/uol/%{projectname}
make build

#------------------------------------------------------------------------------
# I N S T A L L  F I L E S
#------------------------------------------------------------------------------
%install
buildtmp=${GOPATH}/src/github.com/uol/%{projectname}
rm -rf "%{buildroot}"
install -m 755 -d "%{buildroot}"/opt/%{projectname}/bin
install -m 755 ${buildtmp}/%{binaryname} "%{buildroot}"/opt/%{projectname}/bin
install -m 755 -d "%{buildroot}"/lib/systemd/system
install -m 755 ${buildtmp}/rpm/%{projectname}.service "%{buildroot}"/lib/systemd/system/%{projectname}.service
install -m 755 -d "%{buildroot}"/etc/sysconfig/
install -m 755 ${buildtmp}/rpm/%{projectname}.sysconfig "%{buildroot}"/etc/sysconfig/%{projectname}
install -m 755 -d "%{buildroot}"/var/run/%{projectname}
install -m 755 -d "%{buildroot}"/var/lib/%{projectname}

#------------------------------------------------------------------------------
# P R E - I N S T A L L  S C R I P T
#------------------------------------------------------------------------------
%pre

if [ "$( getent group "%{binaryname}" )" == "" ]; then
    groupadd -r "%{binaryname}"
fi

if [ "$( getent passwd "%{binaryname}" )" == "" ]; then
    useradd -d "/opt/%{projectname}" -c "User of mycenae application" \
        -g "mycenae" -s "/sbin/nologin" -r \
        "%{binaryname}"
fi

#------------------------------------------------------------------------------
# P O S T - I N S T A L L  S C R I P T
#------------------------------------------------------------------------------
%post
systemctl mycenae
systemctl daemon-reload
setcap CAP_NET_BIND_SERVICE=+eip /opt/%{projectname}/bin/%{projectname}

#------------------------------------------------------------------------------
# P R E - U N I N S T A L L  S C R I P T
#------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------
# P O S T - U N I N S T A L L  S C R I P T
#------------------------------------------------------------------------------
%postun

#------------------------------------------------------------------------------
# C L E A N  U P
#------------------------------------------------------------------------------
%clean
/bin/rm -rf "%{buildroot}"

#------------------------------------------------------------------------------
# F I L E S
#------------------------------------------------------------------------------
%files
%attr(0755, root, root) /opt/%{projectname}/bin/%{binaryname}
%dir /var/run/%{projectname}
%attr(0755, %{binaryname}, root) /var/run/%{projectname}
%dir /lib/systemd/system
%attr(0755, root, root) /lib/systemd/system/%{projectname}.service
%dir /etc/sysconfig
%attr(0755, root, root) /etc/sysconfig/%{projectname}
%dir /var/lib/%{projectname}
%attr(0755, %{binaryname}, %{binaryname}) /var/lib/%{projectname}

#------------------------------------------------------------------------------
# C H A N G E L O G
#------------------------------------------------------------------------------
%changelog
