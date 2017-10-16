#!/usr/bin/env python
#coding: utf-8
from  SocketServer import  ThreadingMixIn
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from time import ctime,sleep
from threading import current_thread
import threading
import urlparse
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
#import zookeeper
import httplib
import os
import psutil
import json
import socket
import datetime, time
import re
import commands
import space_admin
import collections
import time
from kazoo.client import KazooClient

g_result_list = []

g_memtotal  = 'MemTotal'
g_memfree   = 'MemFree'
g_memxcloud = 'MemXcloud'

g_cpuCore   = 'cpuCore'
g_cpuUsed   = 'cpuUsed'
g_cpuXcloud = 'cpuXcloud'

g_sysinfo  = '0'
g_expand   = 'false'
g_topsql   = '0'
g_duration = ''

g_hdfs_who = 'false'
g_topsql_who = 'false'
g_hdfs = ''
g_dbName   = ''
#g_userName = ''
g_topsql_info = ''
g_topsql_requested = '0'
g_hdfs_info = ''
g_hdfs_requested = '0'
g_mutex_lock = threading.Lock()
def server_unavailable_str(addr):
    print addr
    try:
        host_fqdn = socket.getfqdn(addr)    
        print host_fqdn
        host_ip = socket.gethostbyname(host_fqdn)
        print host_ip
    except Exception, e:
        print Exception,":", e
        print "the /etc/host/  has no host :%s",addr
        host_ip = "None"
    result = '{' \
             '"host":"%s:%s",' \
             '"monitor_status":"off",' \
             '"cpu":{"cores":"N/A","xcloud_cpu":"N/A","cpuUsed":"N/A"},' \
             '"memory":{"MemTotal":"N/A","MemFree":"N/A","MemXcloud":"N/A"' \
             '}}' % (addr, host_ip)
    print "server_unavailable_str===result=====",result
    return result

def get_zookerper_ip_and_port():
    print 'get_zookerper_ip_and_port'
    tree = ET.ElementTree(file='../conf/ds.xml')
    # for elem in tree.iter(tag='flag'):
    root = tree.getroot()
    addr = None
    port = None
    for zk in root.findall('flag'):
        zk_server = zk.find('name').text
        if zk_server == 'zk_server':
            server = zk.find('current').text
            print "zk_server ==========", server
            addr = server.split(':')[0]
            port = server.split(':')[1]
            print 'and zk_ip=%s,---zk_port=%s' %(addr, port)
            break
    return addr, port

def get_dir_structure():
    print "get_dir_structure starting"
    #if not os.path.exists('../upgrade-datadir-xcloud.sh'):
     #   if not os.path.exists('../xcloud-dataversion.sh'):
      #      return 'Old'
    #struct = commands.getoutput("../upgrade-datadir-xcloud.sh -auto status  | awk  '/Data status: /  {print $3}'")
    if not os.path.exists('./dir_struc'):
        return 'Old'

    dir_file = "dir_struc"
    struct = 'Old'
    with open(dir_file, 'r') as f:
        lines = f.readlines()
        struct = lines[0].strip()
    print "pfmon------dir_struct", struct
    return struct

def get_zknode():
    print "get_zknode_new starting"
    zk_node = '/XCLOUD/NODESTATE/NODELIST_FRESH'
    xcloud_root = 'xcloud'
    cluster_name = 'xcloud'
    tree = ET.ElementTree(file = '../conf/ds.xml')
    root = tree.getroot()
    for zk in root.findall('flag'):
        zk_xcloud = zk.find('name').text
        if zk_xcloud == 'xcloud_root_name':
            try:
                xcloud_root = zk.find('current').text
            except Exception:
                xcloud_root = zk.find('current').text
        if zk_xcloud == 'cluster_name':
            try:
                cluster_name = zk.find('current').text
            except Exception:
                cluster_name = zk.find('current').text
    struct = get_dir_structure()
    if struct == 'Old':
        print "zk_node====",zk_node
        return zk_node
    else:
        zk_pre = '/%s/%s' % (xcloud_root, cluster_name)
        zk_node = '%s/XCLOUD/NODESTATE/NODELIST_FRESH' %(zk_pre)
        print "zk_node====",zk_node
        return zk_node

def get_zknode_old():#old version
    print "get_zknode starting"
    zk_node = '/XCLOUD/NODESTATE/NODELIST_FRESH'
    xcloud_root = 'xcloud'
    cluster_name = 'xcloud'
    tree = ET.ElementTree(file = '../conf/ds.xml')
    root = tree.getroot()
    for zk in root.findall('flag'):
        zk_xcloud = zk.find('name').text
        if zk_xcloud == 'xcloud_root_name':
            try:
                xcloud_root = zk.find('current').text
            except Exception:
                xcloud_root = zk.find('current').text
        if zk_xcloud == 'cluster_name':
            try:
                cluster_name = zk.find('current').text
            except Exception:
                cluster_name = zk.find('current').text

    version_tree = ET.ElementTree(file = '../version.xml')
    version_root = version_tree.getroot()
    for version in version_root.findall('flag'):
        line_version = version.find('name').text
        if line_version == 'XCLOUD_BUILD_VERSION':
            xcloud_version = version.find('version').text
            if xcloud_version.find('.') != -1:
                num = xcloud_version.split('.')[1]
                if 1 == int(num):
                    #2.1version
                    small_num = xcloud_version.split('.')[2]
                    if int(small_num) == 2:
                        #2.1.2version
                        struct = get_dir_structure()
                        if 'Old' == struct:
                            print "zknode of 2.1.2===",zk_node
                            return zk_node
                        zk_pre = '/%s/%s' % (xcloud_root, cluster_name)
                        zk_node = '%s/XCLOUD/NODESTATE/NODELIST_FRESH' %(zk_pre)
                        print "zknode of 2.1.2===",zk_node
                        return zk_node
                    elif int(small_num) == 3:
                        #2.1.3
                        struct = get_dir_structure()
                        if 'Old' == struct:
                            print "zknode of 2.1.3===",zk_node
                            return zk_node
                        zk_pre = '/%s/%s' % (xcloud_root, cluster_name)
                        zk_node = '%s/XCLOUD/NODESTATE/NODELIST_FRESH' %(zk_pre)
                        print "zknode of 2.1.3latest===",zk_node
                        return zk_node
                    elif int(small_num) == 1:
                        zk_node = '/XCLOUD/NODESTATE/NODELIST_FRESH'
                        print "zknode of 2.1.1===",zk_node
                        return  zk_node
                    else:
                        print "旧版本"
                else:
                    #2.2以上版本（包含2.2）
                    struct = get_dir_structure()
                    if 'Old' == struct:
                        print "zknode of 2.2==",zk_node
                        return zk_node
                    zk_pre = '/%s/%s' % (xcloud_root, cluster_name)
                    zk_node = '%s/XCLOUD/NODESTATE/NODELIST_FRESH' %(zk_pre)
                    print "zknode of 2.2======",zk_node
                    return zk_node
            else:
                #trunk version
                struct = get_dir_structure()
                print "version_trunk_dir-----",struct
                if struct=='Old':
                    zk_node = '/XCLOUD/NODESTATE/NODELIST_FRESH'
                    print "zknode of trunk==",zk_node
                    return zk_node

                zk_pre = '/%s/%s' % (xcloud_root, cluster_name)
                zk_node = '%s/XCLOUD/NODESTATE/NODELIST_FRESH' %(zk_pre)
                print "zknode of trunk======",zk_node
                return zk_node


def get_active_xclouds_from_zookeeer(addr, port):
    str_connection = "%s:%s" % (addr, port)
    print "str_connection: %s" % str_connection
    zk = zookeeper.init(str_connection)
    zk_node = get_zknode()
    print "zk_node======",zk_node
    #return zookeeper.get_children(zk, "/XCLOUD/NODESTATE/NODELIST_FRESH", None)
    return zookeeper.get_children(zk, zk_node, None)

def get_active_xclouds_from_zookeeer_kazoo(addr, port):
    str_connection = 'hosts=%s:%s' % (addr, port)
    print "str_connection: %s" % str_connection
    zk = KazooClient(hosts='%s:%s' % (addr, port))
    zk.start()
    zk_node = get_zknode()
    print "zk_node====",zk_node
    #children = zk.get_children("/XCLOUD/NODESTATE/NODELIST_FRESH")
    children = zk.get_children(zk_node)
    zk.stop()
    return children


def get_info_remote(addr, history, topsql, sysinfo, port=None):
    try:
        print 'going to fetch data from %s' % addr
        if port is None:
            port = get_xcperf_port()
            print "port===",port
        print "monitoring_port is ==========",port
        ip  = socket.gethostbyname(addr)
        print "ip=====",ip
        conn = httplib.HTTPConnection(ip, port)
        #conn = httplib.HTTPConnection(addr, port)
        global g_sysinfo
        print 'g_sysinfo: %s' % sysinfo
        if sysinfo == '1':
            print "before request sysinfo"
            conn.request('GET', 'index.html?sysinfo=1&expand=false')
            print "after request sysinfo"
        else:
            #print "g_topsql==%d,g_duration==%s" %(int(g_topsql), g_duration)
            print "g_topsql==%d,g_duration==%s" %(int(topsql), history)
            #url = 'index.html?top=%d&history=%s&expand=false&who=true' % (int(g_topsql), g_duration)
            url = 'index.html?top=%d&history=%s&expand=false&who=true' % (int(topsql), history)
            conn.request('GET', url)
            print "after requests top10"
        rsp = conn.getresponse()
        print "rsp==============",rsp
        return rsp.read()
    except Exception, e:
        print "exception=====",Exception, ":", e
        return server_unavailable_str(addr)

def get_xcperf_port():
    print "get_monitoring_port"

    tree = ET.ElementTree(file='../conf/ds.xml')
    #zk_elem = tree.getroot()[115]
    zk_elem = tree.getroot()
    for zk in zk_elem.findall('flag'):
        monitoring_port = zk.find('name').text
        if monitoring_port == 'monitoring_port':
            try:
                port = zk.find('current').text
            except Exception:
                port = zk.find('current').text
            return int(port) 
    #try:
        #file = open('xcperf_port', 'r');
        #xcperf_port = file.read()
        #return int(xcperf_port)
        #port  = int(zk_elem[4].text)
    #except Exception:
        #return int(10011)
        #port  = int (zk_elem[3].text)
    return int(10011)

def get_xcloud_pid():
    # file = open('../bin/pid', 'r');
    file = open('pid', 'r');
    strpid = file.read()
    print 'get_xcloud_pid ret: %s' % strpid
    return int(strpid)

def write_pid_to_file(file_path):
    file = open(file_path, 'w');
    pid = os.getpid()
    print('pid: %s' % pid)
    file.write('%s' % pid)
    return


def collect_xcloud_mem_info(memdict, pid):
    try:
        process = psutil.Process(pid)
        meminfo = process.memory_info()
        # print process.memory_info()
        print "rss: %d Byte" % (meminfo.rss)
        # assert isinstance(rss, object)
        memdict[g_memxcloud] = long(meminfo.rss / 1024)
        return
    except Exception, e:
        memdict[g_memxcloud] = 'N/A'


def collect_sys_mem_info(memdict):
    mem = {}
    f = open("/proc/meminfo")
    lines = f.readlines()
    f.close()
    for line in lines:
        if len(line) < 2: continue
        name = line.split(':')[0]
        var = line.split(':')[1].split()[0]
        mem[name] = long(var)
    memdict[g_memtotal] = mem[g_memtotal]
    memdict[g_memfree] = mem[g_memfree]
    print 'mem_total: %d' % memdict[g_memtotal]
    return

def collect_xcloud_cpu_info(cpudict, pid):
    try:
        process = psutil.Process(pid)
        cpudict[g_cpuXcloud] = process.cpu_percent(1) / psutil.cpu_count()
        return
    except psutil.NoSuchProcess:
        cpudict[g_cpuXcloud] = 'N/A'

def collect_sys_cpu_info(cpudict):
    cpudict[g_cpuCore] = psutil.cpu_count()
    cpudict[g_cpuUsed] = psutil.cpu_percent()
    return
# [in]
# [out] string like: "MemTotal" : "65809859", "MemFree" : "50000059", "MemXcloud" : "10000059"
def collect_sys_cpu_mem_info(memdict, cpudict, pid):
    # sys_mem_total = 0;
    # sys_mem_free = 0;
    collect_sys_mem_info(memdict)
    collect_xcloud_mem_info(memdict, pid)
    collect_sys_cpu_info(cpudict)
    collect_xcloud_cpu_info(cpudict, pid)
    print 'mem_total: %d' % memdict[g_memtotal]
    print 'mem_free: %d' % memdict[g_memfree]
    print 'mem_xcloud: %s' % memdict[g_memxcloud]
    print 'cpu_core: %d' % cpudict[g_cpuCore]
    print 'cpu_used: %f' % cpudict[g_cpuUsed]
    print 'cpu_xcloud: %s' % cpudict[g_cpuXcloud]
    # print 'sys_mem_free: ' + sys_mem_free
    return

def parse_url(strurl):
    print 'enter func parse_url'
    print strurl
    url_dict = collections.OrderedDict()
    pattern_top10_sqls  = re.compile(r'top')
    pattern_sys_info    = re.compile(r'sysinfo')
    pattern_space_usage = re.compile(r'hdfs')
    old_pattern_space_usage = re.compile(r'/\?dbname=(\w+)&username=(\w+)')
    is_top10_sqls  = pattern_top10_sqls.search(strurl)
    is_sys_info    = pattern_sys_info.search(strurl)
    is_space_usage = pattern_space_usage.search(strurl)
    is_old_space_usage = old_pattern_space_usage.search(strurl)

    print "is_top10_sqls------------",is_top10_sqls
    print "is_sys_info--------------",is_sys_info
    print "is_space_usage-----------",is_space_usage
    print "is_old_space_usage-------",is_old_space_usage

    global g_expand
    urldict = urlparse.parse_qs(urlparse.urlparse(strurl).query)
    if urldict.has_key('expand'):
        print urldict['expand']
        for expand in urldict['expand']:
            g_expand = expand
            url_dict['expand'] = expand
    else:
        print 'no expand'



    if is_top10_sqls:
        global g_topsql
        global g_duration
        global g_topsql_who
        url_dict['t_who'] = 'false'

        for sql_num in urldict['top']:
            g_topsql = sql_num
            url_dict['topsql'] = sql_num
        for dura in urldict['history']:
            g_duration = dura
            url_dict['history'] = dura
        if urldict.has_key('who'):
            for l_twho in urldict['who']:
                g_topsql_who = l_twho
                url_dict['t_who'] = l_twho
        print 'top sqls: sql num: %s, history: %s,topwho:%s' % (g_topsql, g_duration,g_topsql_who)
        print is_top10_sqls.group()
    else:
        print 'no top sql'

    if is_sys_info:
        global g_sysinfo
        for sys_info in urldict['sysinfo']:
            g_sysinfo = sys_info
            url_dict['sysinfo'] = sys_info
        print is_sys_info.group()
    else:
        print 'no sys info'

    if is_space_usage:
        global g_hdfs
        global g_hdfs_who
        url_dict['h_who'] = 'false'
    
        for l_hdfs in urldict['hdfs']:
            g_hdfs = l_hdfs
            url_dict['hdfs'] = l_hdfs
        if urldict.has_key('who'):
            for l_who in urldict['who']:
                g_hdfs_who = l_who
                url_dict['h_who'] = l_who
        print 'get hdfs args is %s,hdfs_who===%s'%( url_dict['hdfs'] ,url_dict['h_who'])
        print is_space_usage.group()
    else:
        print ('no space usage')

    if is_old_space_usage:
        url_dict['dbName'] = urldict['dbname'][0]
        url_dict['userName'] = urldict['username'][0]
    else:
        print "no old_space usage"
    return  url_dict


def to_json(data):
    return json.dumps(data)


def to_json_demo():
    jsdata = {"HOSTS" : [{"host" : "172.16.12.59",
                          "memory" : {"MemTotal" : "65809859", "MemFree" : "50000059", "MemXcloud" : "10000059"},
                          "cpu" : {"cores" : "8", "cpuUsed" : "0.59", "xcloud_cpu" : "159.0"}},
                         {"host": "172.16.12.60",
                          "memory": {"MemTotal": "65809860", "MemFree": "50000060", "xcloud_mem": "10000060"},
                          "cpu": {"cores": "8", "cpuUsed": "0.60", "cpuXcloud": "160.0"}}
                         ]}
    return json.dumps(jsdata)
def get_mount():
    print "i 'm  get_mount start!!!"
    mount = commands.getoutput('df -k')
    file_writer = open('mount', 'w')
    file_writer.write(mount)
    file_writer.close()
    file_reader = open('mount')
    file_length = open('mount')
    length = len(file_length.readlines())
    file_length.close()
    count = 0
    result=""

    for line in file_reader:
        count = count +1
        if count == 1:
            continue
        line = ' '.join(line.split())
        if count == length -1:
            result = result + '{' \
                    '"Name":"%s",' \
                    '"Size":"%s",' \
                    '"Used":"%s",' \
                    '"Avail":"%s",' \
                    '"UsedPercent":"%s",' \
                    '"FileSystem":"%s"' \
                    '}' \
                    % \
                    (line.split()[5],
                            line.split()[1],
                            line.split()[2],
                            line.split()[3],
                            line.split()[4],
                            line.split()[0])
            break
        result = result +   '{' \
            '"Name":"%s",' \
            '"Size":"%s",' \
            '"Used":"%s",' \
            '"Avail":"%s",' \
            '"UsedPercent":"%s",' \
            '"FileSystem":"%s"' \
            '}' \
            ',' \
            '\n'\
            % \
            (line.split()[5],
                line.split()[1],
                line.split()[2],
                line.split()[3],
                line.split()[4],
                line.split()[0])
    result = '"mount":[' \
            "%s"\
            ']' \
            % \
            result
    file_reader.close()
    if os.path.exists('mount'):
        os.remove('mount')
    return result


def make_result_single(mem_dict, cpu_dict):
    print "enter func make_result_single"
    host_name = socket.gethostname()
    print "host_name = %s" %(host_name)
    host_fqdn = socket.getfqdn(socket.gethostname())
    host_ip = socket.gethostbyname(host_fqdn)
    mount_dict = get_mount();
    result = '{' \
             '"host":"%s:%s",' \
             '"monitor_status":"on",' \
             '"cpu":{"cores":"%d","xcloud_cpu":"%s","cpuUsed":%s},' \
             '"memory":{"MemTotal":"%d","MemFree":"%d","MemXcloud":"%s"' \
             '},' \
             '%s' \
             '}' % \
             (host_name, host_ip,
              cpu_dict[g_cpuCore], cpu_dict[g_cpuXcloud], cpu_dict[g_cpuUsed],
              mem_dict[g_memtotal], mem_dict[g_memfree], mem_dict[g_memxcloud],mount_dict)
    print result
    return result

def make_topsql_result_total(results, expand, topsql):
    print 'enter func make_topsql_result_total'
    print 'before do anything : %s' % str(results)
    print 'end before do anything'
    len_raw = len(results)
    print "len_raw===",len_raw
    if 0 == len_raw:
        return ''
    results.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))   # 降序
    print "after sort result===%s" %str(results)
    #if g_expand == 'true':
    if expand == 'true':
        rank = 1
        for result in results:
            result['rank'] = rank
            result['runTime'] = str(result['runTime']) + 'ms'
            rank += 1
    #if len_raw < int(g_topsql):
    if len_raw < int (topsql):
        len_results = len_raw
    else:
        #len_results = int(g_topsql)
        len_results = int(topsql)
    print 'len_results: %d' % len_results
    results = results[0:len_results]
    index = 0
    result_total = ''
    #if g_mutex_lock.acquire():
    #if g_expand == 'true':
    if expand == 'true':
        result_total += '{"SQLS":['

        for result in results:
            # print result
            #result_total += str(result)
            result_total += json.dumps(result)
            index = index + 1
            # print 'compare index : %i , len_results : %i' % (index, len_results)
            if (index != len_results):
                result_total += ','
        result_total+=']}'

    else:
        print 'get top sqls'
        for result in results:
            #result_total += str(result)
            result_total += json.dumps(result)
            index = index + 1
            # print 'compare index : %i , len_results : %i' % (index, len_results)
            if (index != len_results):
                result_total += '|'
    return result_total

def make_sysinfo_result_total(results, expand):
    print 'enter func make_sysinfo_result_total'
    print 'before do anything : %s' % str(results)
    print 'end before do anything'

    len_results = len(results)
    print 'len_results: %d' % len_results

    index = 0
    result_total = ''
    if expand == 'true':
        result_total += '{"HOSTS":['
        for result in results:
            # print result
            result_total += str(result)
            index = index + 1
            # print 'compare index : %i , len_results : %i' % (index, len_results)
            if (index != len_results):
                result_total += ','
        result_total+=']}'

    else:
        for result in results:
            print 'get sys info'
            result_total += str(result)
    return result_total

title_regex = r"^\w\d{4} \d\d:\d\d:\d\d\.\d{6}.+logging\.h:88\]"   # S0217 23:18:45.558907  9105 logging.h:88]
queryId_regex = r" query_id\[.+:?.+:?.+:0\]"   #  query_id[3e0c10ac:58a71453:848:0]
inflight_regex = r", is_inflight\[\b(true|false)\b\]"    # , is_inflight[true]
#stmt_regex = queryId_regex + r", stmt\[.+\]" + inflight_regex   # queryId_regex + stmt[sql], is_inflight[true]
stmt_regex = queryId_regex + r", stmt\[(\S|\s)+" + inflight_regex
#startTime_regex = r", start_time\[\w{3} \w{3} \d\d \d\d:\d\d:\d\d \d{4}\]"   # , start_time[Fri Feb 17 23:18:43 2017]
startTime_regex = r", start_time\[\w{3} \w{3} .. \d\d:\d\d:\d\d \d{4}\]"   # , start_time[Fri Feb 17 23:18:43 2017]
#stopTime_regex = r", stop_time\[\w{3} \w{3} \d\d \d\d:\d\d:\d\d \d{4}\]"   # , stop_time[Fri Feb 17 23:18:43 2017]
stopTime_regex = r", stop_time\[\w{3} \w{3} .. \d\d:\d\d:\d\d \d{4}\]"   # , stop_time[Fri Feb 17 23:18:43 2017]
runTime_regex = r", run_time\[\d+\(ms\)\]"    # , run_time[2283(ms)]
end_regex = r"\. Query Used Time Total \[[\w.]+\]\. ?\b(success|failed)\b\.\(code=\d+\..+$"   # . Query Used Time Total [2s304ms].success.(code=9.)
standard_regex = title_regex + stmt_regex + startTime_regex + stopTime_regex + runTime_regex + end_regex

class ParserSQL():
    def __init__(self, dirPath, rectime, topN):
        self._dir   = dirPath
        self._fileList = []
        self._time  = rectime
        self._stopTime = datetime.datetime.now()
        self._startTime = datetime.datetime.now()
        self._count = topN
        self._sqls  = []   # add dict sql, count+1

    def initReCompile(self):
        self.standard_pattern = re.compile(standard_regex)
        self.queryId_pattern  = re.compile(queryId_regex)
        self.stmt_pattern     = re.compile(stmt_regex)
        self.startTime_pattern= re.compile(startTime_regex)
        self.stopTime_pattern = re.compile(stopTime_regex)
        self.usedTime_pattern = re.compile(end_regex)
        self.runTime_pattern  = re.compile(runTime_regex)


    '''
    Only need use this interface
    '''
    def parser(self):
        self.parserDate()
        self.findFiles()
        print "self._fileList====",self._fileList
        self.initReCompile()
        self.findSql()
        #if len(self._sqls) == self._count+1:
         #   self._sqls = self._sqls[:-1]
        self._sqls.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))   # 降序
        if len(self._sqls) == self._count+1:#改成先降序再取出N条sql
            self._sqls = self._sqls[:-1]
        name = socket.getfqdn(socket.gethostname())
        addr = socket.gethostbyname(name)
        rank = 1
        for sql in self._sqls:
            # sql['rank'] = rank
            sql['host'] = addr
            # sql['runTime'] = str(sql['runTime']) + 'ms'
            rank += 1
        #sqlsDict = {'SQLS':self._sqls}
        #sqlsJson = json.dumps(sqlsDict)
        print "parser return before===",self._sqls
        return self._sqls

    '''
    compare fileList,accoring to file ctime, DSC
    '''
    def compareFile(self, file1, file2):
        stat_file1 = os.stat(self._dir + "/" + file1)
        stat_file2 = os.stat(self._dir + "/" + file2)
        if stat_file1.st_ctime > stat_file2.st_ctime:
            return -1
        elif stat_file1.st_ctime < stat_file2.st_ctime:
            return 1
        else:
            return 0

    def parserDate(self):
        tmpTime = '-'
        tmpTime += self._time[:-1]
        if self._time.find('d'):
            self._startTime = self._startTime + datetime.timedelta(days=int(tmpTime))
        elif self._time.find('h'):
            self._startTime = self._startTime + datetime.timedelta(hours=int(tmpTime))
        elif self._time.find('m'):
            self._startTime = self._startTime + datetime.timedelta(minutes=int(tmpTime))
        elif self._time.find('s'):
            self._startTime = self._startTime + datetime.timedelta(seconds=int(tmpTime))
        else:
            pass


    '''
    find STMT files
    '''
    def findFiles(self):
        files = []
        iterms = os.listdir(self._dir)
        for iterm in iterms:
            if iterm.find("STMT_.log") > -1 and iterm.find("tar.gz") == -1:
                files.append(iterm)
                files.sort(self.compareFile)

        for afile in files:
            stat_afile = os.stat(self._dir + "/" + afile)
            if datetime.datetime.fromtimestamp(stat_afile.st_ctime) > self._startTime:
                self._fileList.append(afile)
            else:
                break


    '''
    find recently time sql
    '''
    def findSql(self):
        for oneFile in self._fileList:
            lines = []
            with open(self._dir + "/" + oneFile) as f:
                linet = ''
                for line1 in f:
                    if line1.find('\r\n') != -1:
                        print "this line contain huiche",line1
                        line1 = line1.replace('\r\n', ' ')
                        linet = linet+line1
                    else:
                        if linet != '':
                            linet = linet+line1
                            print "after cut huiche ----",linet
                            lines.append(linet)
                            linet = ''
                        else:
                            lines.append(line1)

                for line in lines:
                    print "will ParserSQL....",line
                    isCorrect = self.isCorrect(line)
                    if isCorrect:
                        print "ParserSQL line is ......",line
                        curTimeMatch = self.stopTime_pattern.search(line)
                        curTime = curTimeMatch.group()     # ', stop_time[Fri Feb 17 22:03:58 2017]'
                        curTime = curTime[12:-1]
                        curTime = datetime.datetime.strptime(curTime, '%a %b %d %H:%M:%S %Y')
                        if self._startTime < curTime and curTime < self._stopTime:
                            self.addCmpSQL(line)
                        else:
                            continue
                    else:
                        continue
            del lines[:]

    def isCorrect(self, srcStr):
        if self.standard_pattern.search(srcStr) is not None:
            return True
        else:
            return False

    '''
    add sql to self._sqls,  and compare
    '''
    def addCmpSQL(self, srcStr):
        runTime = self.runTime_pattern.search(srcStr).group()[11:-5]
        queryId = self.queryId_pattern.search(srcStr).group()[10:-1]
        startTime = self.startTime_pattern.search(srcStr).group()[13:-1]
        stopTime  = self.stopTime_pattern.search(srcStr).group()[12:-1]
        # Mon Jul  3 17:39:58 2017  ==> 2017-07-03 18:05:06  ; str=>ptime=>str
        tmpStartTime = time.strptime(startTime,'%a %b %d %H:%M:%S %Y')
        tmpStopTime = time.strptime(stopTime,'%a %b %d %H:%M:%S %Y')
        startTime = time.strftime('%Y-%m-%d %H:%M:%S', tmpStartTime)
        stopTime = time.strftime('%Y-%m-%d %H:%M:%S', tmpStopTime)

        usedTime  = self.usedTime_pattern.search(srcStr).group()
        usedTime  = usedTime.split(']')[0][25:]
        stmt      = self.stmt_pattern.search(srcStr).group()
        stmt      = stmt.split(', stmt[')[1].split('], is_inflight[')[0]
        sqlDict = {'queryID':queryId, 'startTime':startTime, 'stopTime':stopTime, 'usedTime':usedTime, 'runTime':runTime, 'sql':stmt, 'host':"", 'rank':0}
        print "-------",sqlDict,"------"
        if len(self._sqls) < self._count+1:  # _sqls存放count+1个元素,+1元素用于存放新的元素，以便排序
            self._sqls.append(sqlDict)    # 此时len(_sqls) = _count+1
        else:
            self._sqls[self._count] = sqlDict    # count+1 是最小的runTime
            self._sqls.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))   # 降序


# DIR = "/home/zhanghaoyu/xpkg_bingfa/xpkg/log"
# DIR = "/home/zhanghaoyu/python"
# DIR = "."
# testParser = ParserSQL(DIR, '60d', 10)
# res = testParser.parser()
# Create custom HTTPRequestHandler class
class KodeFunHTTPRequestHandler (BaseHTTPRequestHandler):
    # handle GET command
    def do_GET(self):
        global g_sysinfo
        global g_topsql
        global g_expand
        global g_hdfs
        global g_hdfs_who
        global g_topsql_who
        global g_topsql_info
        global g_hdfs_info
        global g_mutex_lock
        g_hdfs_who = 'false'
        g_topsql_who = 'false'
        g_hdfs = '0'
        g_sysinfo = '0'
        g_topsql = '0'
        g_expand = 'false'
        g_topsql_who = 'false' 
        g_hdfs_who = 'false'

        l_expand = ''
        l_hdfs = ''
        l_topsql = '0'
        l_twho = ''
        l_hwho = ''
        l_history = ''
        l_sysinfo = ''
        l_dbName = ''
        l_userName = ''
        if self.path.endswith("/favicon.ico"):
            self.send_response(404)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            print "request error.......404"
            return
        try:
            # send code 200 response
            print self.path
            urldict = parse_url(self.path)
            for key in urldict:
                if key == 'hdfs':
                    l_hdfs = urldict['hdfs']
                elif key == 'topsql':
                    l_topsql = urldict['topsql']
                elif key == 'expand':
                    l_expand = urldict['expand']
                elif key == 't_who':
                    l_twho = urldict['t_who']
                elif key == 'h_who':
                    l_hwho = urldict['h_who']
                elif key == 'sysinfo':
                    l_sysinfo = urldict['sysinfo']
                elif key == 'history':
                    l_history = urldict['history']
                elif key == 'dbName':
                    l_dbName = urldict['dbName']
                elif key == 'userName':
                    l_userName = urldict['userName']

            self.send_response(200)

            # send header first
            self.send_header('Content-type', 'text-html')
            self.end_headers()


            memlist = [(g_memtotal, 0), (g_memfree, 0), (g_memxcloud, 0)]
            memdict = dict(memlist)
            cpulist = [(g_cpuCore, 0), (g_cpuUsed, 0), (g_cpuXcloud, 0)]
            cpudict = dict(cpulist)

            result_list = []
            pid = get_xcloud_pid()
            print 'xcloud is running at pid: %d' % pid
            
            if l_sysinfo == '1':
                collect_sys_cpu_mem_info(memdict, cpudict, pid)
                result = make_result_single(memdict, cpudict)
                result_list.append(result)

            #if g_topsql != '0':
            if l_topsql != '0':
                if l_twho == 'false':
                    while 1:
                        print "i 'm g_topsql  who =false"
                        #if g_mutex_lock.acquire():
                        if g_topsql_info != '':
                            print "g_topsql_info==============",g_topsql_info
                            self.wfile.write(g_topsql_info)
                             #   g_mutex_lock.release()
                            return
                            #g_mutex_lock.release()

                dir = '../log'
                #print 'new ParserSQL(%s, %s, %d)' % (dir, g_duration, int(g_topsql))
                print 'new ParserSQL(%s, %s, %d)' % (dir, l_history, int(l_topsql))
                #topSqlParser = ParserSQL('../log', g_duration, int(g_topsql))
                topSqlParser = ParserSQL('../log', l_history, int(l_topsql))
                print "topSqlParser==============",topSqlParser
                sql_res = topSqlParser.parser()
                print "sql_res:"
                print sql_res
                result_list.extend(sql_res)
                print "result_list=========",result_list
                # for res in sql_res:
                    # result_list.append(res)

            #if g_expand == 'true':
            if l_expand == 'true':
                addr, port = get_zookerper_ip_and_port()
                print addr
                print port
                # print 'get zookeeper ip, port from ds.xml: %s : %s' % addr, port
                if addr == None or port == None:
                    print "getZK port and ip error from  ds.xml"
                    return
                print 'fetch active xclouds from zookeeper:'
                active_nodes = get_active_xclouds_from_zookeeer_kazoo(addr, port)
                print active_nodes

                host_name = socket.gethostname()   # eg.  host_name:xcloud9
                host_fqdn = socket.getfqdn(socket.gethostname())
                host_ip = socket.gethostbyname(host_fqdn)
                print "result_list===",result_list
                print 'host_name: %s:%s' % (host_name, host_ip)
                zk = KazooClient(hosts='%s:%s' % (addr, port))
                if not zk.connected:
                    zk.start()
                zk_node = get_zknode()

                for node in active_nodes:
                    if zk.connected:
                        print "zk connected success"
                    child_dir = zk_node+'/'+node
                    datas = zk.get(child_dir)
                    print "monitoring_port======",datas
                    monitor_port = 0
                    for data in datas:
                        print "get monitor_port by split data",data
                        try:
                            d = json.loads(data)
                            print "dddddddd",d
                            monitor_port = d.get("monitor_port")
                            print "get monitor_port from zk===",monitor_port
                            if monitor_port != 0:
                                break
                        except Exception:
                            continue

                    print "node_split_before====",node    #eg.  node: xcloud9_17389   
                    if node.find('_') != -1: 
                        s_node = node.split('_')
                        node = s_node[0]
                        print "node_split_after===", node  #eg.  node:xcloud9
                    if None == monitor_port or 0 == monitor_port:
                        #continue
                        if node == host_name:
                            continue
                        else:
                            str_result = get_info_remote(node, l_history, l_sysinfo, l_topsql)
                            print "str_result ==== ",str_result
                            if l_sysinfo == '1':
                                result_list.append(str_result)
                            if l_topsql != '0':
                                if '' == str_result:
                                    continue
                                for result in str_result.split('|'):
                                    result_list.append(eval(result))
                    else:
                        if get_xcperf_port() == monitor_port and node == host_name:
                            continue
                        if l_sysinfo == '1' and node == host_name:
                            continue
                        str_result = ''
                        if monitor_port == None or 0 == monitor_port:
                            str_result = get_info_remote(node, l_history, l_sysinfo, l_topsql)
                        else:
                            str_result = get_info_remote(node, l_history, l_topsql, l_sysinfo, monitor_port)
                        print "str_result ==== ",str_result
                        #if g_sysinfo == '1':
                        if l_sysinfo == '1':
                            result_list.append(str_result)
                        #if g_topsql != '0':
                        if l_topsql != '0':
                            if '' == str_result:
                                continue
                            for result in str_result.split('|'):
                                result_list.append(eval(result))
                zk.stop()
            #if g_sysinfo == '1':
            if l_sysinfo == '1':
                data = make_sysinfo_result_total(result_list, l_expand)
                print "sysinfo======",data
                print "sysinfo===1"
                self.wfile.write(data)

            #if g_topsql != '0':
            if l_topsql != '0':
                print "result_list==",result_list
                data = make_topsql_result_total(result_list, l_expand, l_topsql)
                print "data_topsql==========",data
                if data == '':
                    #if g_expand == 'true':
                    if l_expand == 'true':
                        data = '{'\
                                '"SQLS":'\
                                '{'\
                                '"host":""'\
                                ','\
                                '"stopTime":""'\
                                ','\
                                '"queryID":""'\
                                ','\
                                '"startTime":""'\
                                ','\
                                '"sql":""'\
                                ','\
                                '"usedTime":""'\
                                ','\
                                '"runTime":""'\
                                ','\
                                '"rank":""'\
                                '}'\
                                '}'
                self.wfile.write(data)
                return

            if l_hdfs == '1':
                if 'false' == l_hwho:
                    print "l_hwho===",l_hwho
                    while 1:
                        #if g_mutex_lock.acquire():
                        print " g_hdfs_info=====",g_hdfs_info
                        if g_hdfs_info != '':
                            self.wfile.write(g_hdfs_info)
                           # g_mutex_lock.release()
                            return
                       # g_mutex_lock.release()
                print "l_hwho===",l_hwho
                data = None
                spaceUsage = space_admin.SpaceUsage()
                ret, res = spaceUsage.get_xcloud_info_from_dhfs()
                if ret == -1:
                    data = res
                    print "hdfs return -1, res===",res
                    data = '{'\
                            '"xcloud_hdfs_info":'\
                            '{'\
                            '"quota":""'\
                            ','\
                            '"remaining_quota":""'\
                            ','\
                            '"space_quota":""'\
                            ','\
                            '"remaining_space_quota":""'\
                            ','\
                            '"dir_count":""'\
                            ','\
                            '"file_count":""'\
                            ','\
                            '"content_size":""'\
                            ','\
                            '"xcloud_dir":""'\
                            '}'\
                            ','\
                            '"xcloud":""'\
                            '}'
                else:
                    #for r in res:
                    data = json.dumps(res)
                print data
                self.wfile.write(data)
            if l_dbName != '' and l_userName != '':
                data = None
                spaceUsage = space_admin.SpaceUsage()
                ret, res = spaceUsage.do_space_usage_old(l_dbName, l_userName)
                if ret == -1:
                    data = res
                else:
                    data = json.dumps(res)
                print "old_hdfs===",data
                self.wfile.write(data)

            return
        except IOError:
            self.send_error(404, 'file not found')


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass

def run():
    print('http server is starting...')

    # ip and port of servr
    # by default http server port is 80
    host_fqdn = socket.getfqdn(socket.gethostname())
    host_ip = socket.gethostbyname(host_fqdn)
    port = get_xcperf_port()
    server_address = (host_ip, port)
    #httpd = HTTPServer(server_address, KodeFunHTTPRequestHandler)
    httpd = ThreadedHTTPServer(server_address, KodeFunHTTPRequestHandler)
    print('http server is running...')
    pid_file = 'pid_mon'
    write_pid_to_file(pid_file)
    httpd.serve_forever()
    print "the end of  server_forver "


def requesthdfs(request):
    global g_hdfs_requested
    global g_hdfs_info
    global g_mutex_lock
    print "request====",request
    host_fqdn = socket.getfqdn(socket.gethostname())
    port = get_xcperf_port()
    thread = threading.current_thread()
    print "hdfs threading_ID====",thread.getName()

    sleep(10)
    while 1:
        conn = httplib.HTTPConnection(host_fqdn, port)
        conn.request('GET', 'index.html?hdfs=1&who=true')
        rsp = conn.getresponse()
        g_hdfs_info = rsp.read()

        print "g_hdfs_info =====",g_hdfs_info
        sleep(7200)

def requesttopsql(request):
    global g_topsql_requested
    global g_topsql_info
    global g_mutex_lock
    print "request====",request
    host_fqdn = socket.getfqdn(socket.gethostname())
    port = get_xcperf_port()
    thread = threading.current_thread()
    print "topN threading_ID=====",thread.getName()

    sleep(10)
    while 1:
        conn = httplib.HTTPConnection(host_fqdn, port) 
        url = 'index.html?top=10&history=24h&who=true&expand=true'
        conn.request('GET', url)
        rsp = conn.getresponse()
        g_topsql_info = rsp.read()

        print "g_topsql_info===",g_topsql_info
        sleep(600)
        redirectpfmonlog()

def redirectpfmonlog():
    filePath = '../log/pfmon.log'
    if os.path.exists(filePath):
	    if os.path.getsize(filePath)/1024/1024 > 1000:
	        os.system('>../log/pfmon.log')

if __name__ == '__main__':
    threads = []
    #t1 = threading.Thread(target=run)
    #threads.append(t1)
    t2 = threading.Thread(target=requesthdfs,args={'requesthd',})
    threads.append(t2)
    t3 = threading.Thread(target=requesttopsql,args={'requesttopsql',})
    threads.append(t3)
    #t4 = threading.Thread(target=redirectpfmonlog)
    #threads.append(t4)

    for t in threads:
        t.setDaemon(True)
        t.start()
    run()
    pass
