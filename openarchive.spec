%define major_ver 1
%define minor_ver 0
%define patch_lvl 0
%define dot_ver   %{major_ver}.%{minor_ver}.%{patch_lvl}

Name:           openarchive 
Version:        %{dot_ver}
Release:        1%{?dist}
Summary:        Open archive for performing data management operations
Group:          Development
License:        GPLv3+ 
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  gcc-c++
BuildRequires:  make

%description
This is a module for archiving the managed data for various data management
platforms. Support for different data management products can be added by 
providing the required plugins. An example plugin will be provided for 
Commvault data management platform.

%package devel
Summary: openarchive C++ headers and shared development libraries
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description devel
Headers and shared object symlinks for openarchive library.

%prep
%setup -q

%build
make all

%install
rm -rf $RPM_BUILD_ROOT
%make_install

%post
umask 007
/sbin/ldconfig > /dev/null 2>&1

%postun
umask 007
/sbin/ldconfig > /dev/null 2>&1

%files
%defattr(-, root, root, -)
/usr/local/lib/*.so*
/usr/local/bin/*
/etc/ld.so.conf.d/*

%files devel
%defattr(-, root, root, -)
%{_includedir}/openarchive

%changelog
* Sat Oct 14 2017 Ram Ankireddypalle <areddy@commvault.com>
- First openarchive package
