#!/usr/bin/env python
#coding: utf-8

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

def get_xcperf_port():
    print "get_monitoring_port"
    tree = ET.ElementTree(file='../conf/ds.xml')
    zk_elem = tree.getroot()
    for zk in zk_elem.findall('flag'):
        monitoring_port = zk.find('name').text
        if monitoring_port == 'monitoring_port':
            try:
                port = zk.find('current').text
            except Exception:
                port = zk.find('current').text
            return int(port)
    return int(10011)

def get_xcloud_pid():
    file = open('pid', 'r');
    strpid = file.read()
    print 'get_xcloud_pid ret: %s' % strpid
    return int(strpid)
