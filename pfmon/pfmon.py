#!/usr/bin/env python
#coding: utf-8
from  SocketServer import  ThreadingMixIn
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from time import sleep
import threading
import urlparse
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
#import zookeeper
import httplib
import os
import subprocess
import psutil
import json
import socket
import re
import commands
import collections
from kazoo.client import KazooClient

import parserSQL
import space_admin
import check_info
import myUtil

g_result_list = []

g_memtotal  = 'MemTotal'
g_memfree   = 'MemFree'
g_memxcloud = 'MemXcloud'

g_cpuCore   = 'cores'
g_cpuUsed   = 'cpuUsed'
g_cpuXcloud = 'xcloud_cpu'

g_sysinfo  = '0'
g_checkinfo  = '0'
g_expand   = 'false'
g_topsql   = '0'
g_duration = ''

g_hdfs_who = 'false'
g_topsql_who = 'false'
g_hdfs = ''
g_dbName   = ''
g_topsql_info = ''
g_topsql_requested = '0'
g_hdfs_info = ''
g_hdfs_requested = '0'
g_mutex_lock = threading.Lock()
g_sys_info = ''
def server_unavailable_str(addr):
    try:
        host_fqdn = socket.getfqdn(addr)
        host_ip = socket.gethostbyname(host_fqdn)
    except Exception, e:
        print Exception,":", e
        print "the /etc/host/  has no host :%s",addr
        host_ip = "None"
        '''
    result = '{' \
            '"host":"%s:%s",' \
            '"monitor_port":"%s",' \
            '"xcloud_pid":"%s",' \
             '"monitor_status":"off",' \
             '"cpu":{"cores":"N/A","xcloud_cpu":"N/A","cpuUsed":"N/A"},' \
             '"memory":{"MemTotal":"N/A","MemFree":"N/A","MemXcloud":"N/A"' \
             e}}' % (addr, host_ip,host_monitor,xcloud_pid)
             '''
    result = ""
    print "server_unavailable_str===result=====",result
    return result

def get_zookerper_ip_and_port():
    print 'get_zookerper_ip_and_port'
    tree = ET.ElementTree(file='../conf/ds.xml')
    # for elem in tree.iter(tag='flag'):
    root = tree.getroot()
    servers = None
    for zk in root.findall('flag'):
        zk_server = zk.find('name').text
        if zk_server == 'zk_server':
            servers = zk.find('current').text
            #server = server.split(':')
            #addr = server[0]
            #port = server[1]
            #print 'and zk_ip=%s,---zk_port=%s' %(addr, port)
            return servers
            break
    return servers

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
        if lines == '':
            return  'Old'
        struct = lines[0].strip()
    print "pfmon------dir_struct", struct
    return struct

def get_zknode(servers=None):
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
        zk_nodes = collections.OrderedDict()
        zk_nodes['xcloud'] = zk_node
        return zk_nodes
    else:
        zk_nodes = collections.OrderedDict()
        zk_pre = '/%s/%s' % (xcloud_root, cluster_name)
        zk_node = '%s/XCLOUD/NODESTATE/NODELIST_FRESH' %(zk_pre)
        if servers != None:#topN 要返回所有进程组的znode
            zk = KazooClient(hosts='%s' %(servers))
            try:
                zk.start()
                zk_root_name = '/%s' %(xcloud_root)
                zk_clusters_name = zk.get_children(zk_root_name)
                for zk_cluster_name in zk_clusters_name:
                    z_pre = '/%s/%s' % (xcloud_root, zk_cluster_name)
                    print "get_zknode:z_pre===",z_pre
                    z_node = '%s/XCLOUD/NODESTATE/NODELIST_FRESH' %(z_pre)
                    print "get_zknode: znode===",z_node
                    zk_nodes[z_pre] = z_node
                zk.stop()
            except Exception as e: 
                print "get_zknode Exception is:",e 
                zk.stop()
                zk_nodes[zk_pre] = zk_node
            print "zk_nodes:", zk_nodes
            return zk_nodes
        zk_nodes[zk_pre] = zk_node
        print "zk_node====",zk_nodes
        return zk_nodes

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

def get_active_xcloudinfo_from_zookeer(servers,topsql = '0'):
    zk_nodes = get_zknode()
    if topsql != '0':
        zk_nodes = get_zknode(servers)
    result = collections.OrderedDict()
    count = 0
    str_connection = 'hosts=%s' % (servers)
    zk = KazooClient(hosts='%s' %(servers))
    try:
        zk.start()
        for zk_name in zk_nodes:
            zk_node = zk_nodes[zk_name]
            children = zk.get_children(zk_node)
            for child in children:
                child_dir = zk_node + "/" + child
                datas = zk.get(child_dir)
                monitor_port = 0
                for data in datas:
                    try:
                        d = json.loads(data)
                        monitor_port = d.get("monitor_port")
                    except Exception as e:
                        print 'get_monitor_port from zk  except=%s,monitor_port=%s' %(e,monitor_port)
                if None == monitor_port or 0 == int(monitor_port):
                    monitor_port = myUtil.get_xcperf_port()
                    result[child] = monitor_port
                else:
                    result[child] = monitor_port
                count = count + 1 
        if zk.connected:
            zk.stop()
    except Exception as e:
        print "get_active_xcloudinfo_from_zookeer error:",e
        if zk.connected:
            zk.stop()
            print "get_active_xcloudinfo_from_zookeer exception and zk.stop"

    return result, count


#获取节点的信息，eg  monitoring_port，session_port等
#def get_active_xcloudinfo_from_zookeer_kazoo(addr, port, child_dir):
#    str_connection = 'hosts=%s:%s' % (addr, port)
#    zk = KazooClient(hosts='%s:%s' % (addr, port))
#    zk.start()
#    try:
#        datas = zk.get(child_dir)
#        zk.stop()
#    except Exception:
#        zk.stop()
#    return datas

#def get_active_xclouds_from_zookeeer_kazoo(addr, port):
#    str_connection = 'hosts=%s:%s' % (addr, port)
#    print "str_connection: %s" % str_connection
#    zk = KazooClient(hosts='%s:%s' % (addr, port))
#    zk.start()
#    zk_node = get_zknode()
#    print "zk_node====",zk_node
#    #children = zk.get_children("/XCLOUD/NODESTATE/NODELIST_FRESH")
#    children = zk.get_children(zk_node)
#    zk.stop()
#    return children


def get_info_remote(addr, history, topsql, sysinfo, checkinfo, port=None):
    try:
        print 'going to fetch data from %s' % addr
        if port is None:
            port = myUtil.get_xcperf_port()
        ip  = socket.gethostbyname(addr)
        conn = httplib.HTTPConnection(ip, port)
        #conn = httplib.HTTPConnection(addr, port)
        if sysinfo == '1':
            conn.request('GET', 'index.html?sysinfo=1&expand=false')
        elif checkinfo == '1':
            conn.request('GET', 'index.html?checkinfo=1&expand=false')
        else:
            #url = 'index.html?top=%d&history=%s&expand=false&who=true' % (int(g_topsql), g_duration)
            url = 'index.html?top=%d&history=%s&expand=false&who=true' % (int(topsql), history)
            conn.request('GET', url)
        rsp = conn.getresponse()
        return rsp.read()
    except Exception, e:
        print "exception=====",Exception, ":", e
        return server_unavailable_str(addr)


def write_pid_to_file(file_path):
    file = open(file_path, 'w');
    pid = os.getpid()
    file.write('%s' % pid)
    return


def collect_xcloud_mem_info(memdict, pid):
    try:
        process = psutil.Process(pid)
        meminfo = process.memory_info()
        # print process.memory_info()
        # assert isinstance(rss, object)
        memdict[g_memxcloud] = long(meminfo.rss / 1024)
        return
    except Exception:
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
    # print 'sys_mem_free: ' + sys_mem_free
    return

def parse_url(strurl):
    print 'enter func parse_url'
    url_dict = collections.OrderedDict()
    pattern_top10_sqls  = re.compile(r'top')
    pattern_sys_info    = re.compile(r'sysinfo')
    pattern_check_info    = re.compile(r'checkinfo')
    pattern_space_usage = re.compile(r'hdfs')
    old_pattern_space_usage = re.compile(r'/\?dbname=(\w+)&username=(\w+)')
    is_top10_sqls  = pattern_top10_sqls.search(strurl)
    is_sys_info    = pattern_sys_info.search(strurl)
    is_check_info    = pattern_check_info.search(strurl)
    is_space_usage = pattern_space_usage.search(strurl)
    is_old_space_usage = old_pattern_space_usage.search(strurl)


    global g_expand
    urldict = urlparse.parse_qs(urlparse.urlparse(strurl).query)
    if urldict.has_key('expand'):
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
        print is_top10_sqls.group()
    else:
        print 'no top sql'

    if is_sys_info:
        global g_sysinfo
        url_dict['s_who'] = 'false'
        for sys_info in urldict['sysinfo']:
            g_sysinfo = sys_info
            url_dict['sysinfo'] = sys_info
        #if urldict.has_key('who'):
         #   for l_who in urldict['who']:
          #      url_dict['s_who'] = l_who

        print is_sys_info.group()
    else:
        print 'no sys info'

    if is_check_info:
        global g_checkinfo
        for ck_info in urldict['checkinfo']:
            g_checkinfo = ck_info
            url_dict['checkinfo'] = ck_info
        print is_check_info.group()
    else:
        print 'no check info'

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
    host_name = socket.gethostname()
    host_fqdn = socket.getfqdn(socket.gethostname())
    host_ip = socket.gethostbyname(host_fqdn)
    host_monitor = myUtil.get_xcperf_port()
    xcloud_pid = myUtil.get_xcloud_pid()
    # mount_dict = get_mount()
    result = {}
    sysinfo = check_info.CollectSysInfo()
    result['mount'] = sysinfo.collect_sys_disk_info_by_psutil()
    result['host'] = host_name+":"+host_ip
    result['monitor_port'] = host_monitor
    result['xcloud_pid'] = xcloud_pid
    result['monitor_status'] = "on"
    result['cpu'] = cpu_dict 
    result['memory'] = mem_dict
    result = json.dumps(result)
    return result
    mount_dict = check_info.get_sys_disk_info()
    mount_dict = json.dumps(mount_dict)
    result = '{' \
            '"host":"%s:%s",' \
            '"monitor_port":"%s",' \
            '"xcloud_pid":"%s",' \
             '"monitor_status":"on",' \
             '"cpu":{"cores":"%d","xcloud_cpu":"%s","cpuUsed":%s},' \
             '"memory":{"MemTotal":"%d","MemFree":"%d","MemXcloud":"%s"' \
             '},' \
             '%s' \
             '}' % \
             (host_name, host_ip,host_monitor,xcloud_pid,
              cpu_dict[g_cpuCore], cpu_dict[g_cpuXcloud], cpu_dict[g_cpuUsed],
              mem_dict[g_memtotal], mem_dict[g_memfree], mem_dict[g_memxcloud],mount_dict)
    return result

def make_topsql_result_total2(results, expand, topsql):
    len_raw = len(results)
    print "len_raw===",len_raw
    if 0 == len_raw:
        return ''
    results.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))   # 降序

    if expand == 'true':
        rank = 1
        for result in results:
            result["rank"] = str(rank)   # harvey
            result["runTime"] = str(result["runTime"]) + 'ms'
            rank += 1

    if len_raw < int (topsql):
        len_results = len_raw
    else:
        len_results = int(topsql)

    results = results[0:len_results]
    result_total = {}                # harvey
    result_total["SQLS"] = results   # harvey
    return json.dumps(result_total)  # harvey
    """
    if expand == 'true':
        result_total += '{"SQLS":['
        for result in results:
            result_total += json.dumps(result)
            index = index + 1
            if (index != len_results):
                result_total += ','
        result_total+=']}'
    else:
        print 'get top sqls'
        for result in results:
            result_total += json.dumps(result)
            index = index + 1
            if (index != len_results):
                result_total += '|'
    return result_total
    """
def make_topsql_result_total(results, expand, topsql):
    result_total = {}   # harvey
    print 'enter func make_topsql_result_total'
    len_raw = len(results)
    print "len_raw===",len_raw
    if 0 == len_raw:
        return ''
    if expand == 'false':
        print "huancun sql ziji...",results
        len_raw = len(results)
        if len_raw < int(topsql):
            len_results = len_raw
        else:
            len_results = int(topsql)

        results = results[0:len_results]
        result_total["SQLS"] = results   # harvey
        return json.dumps(result_total)  # harvey
    else:
        print "huizong sql....",results
        rank = 1
        results.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))
        for result in results:
            result['rank'] = str(rank)     # harvey
            result['runTime'] = str(result['runTime']) + 'ms'
            rank += 1
        len_raw = len(results)
        if len_raw < int(topsql):
            len_results = len_raw
        else:
            len_results = int(topsql)
        results = results[0:len_results]
        result_total["SQLS"] = results   # harvey
        return json.dumps(result_total)  # harvey
        """
        result_total += '{"SQLS":['
        for result in results:
            print "expand=true, result==",result
            result_total += json.dumps(result)
            index = index + 1
            if (index < len_results):
                result_total += ','
            else:
                break

        result_total+=']}'
        return result_total
        """

def make_sysinfo_result_total(results, expand):

    len_results = len(results)

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

def make_checkinfo_result_total(results, expand):

    len_results = len(results)

    index = 0
    result_total = ''
    if expand == 'true':
        result_total += '{"CHECKS":['
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
            print 'get check info'
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

def get_default_sql():
    res = '{'\
          '"SQLS":'\
          '{'\
          '"host":""'\
          ','\
          '"minitor_port":""' \
          ',' \
          '"xcloud_pid":""' \
          ',' \
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
    return res

def get_datas_from_nodes(node, history, topsql, sysinfo, checkinfo, monitor_port, result_list, mutex_lock):
#    node = dict_args["node"]
#    monitor_port = dict_args["monitor_port"]
#    history = dict_args["history"]
#    topsql = dict_args["topsql"]
#    sysinfo = dict_args["sysinfo"]
#    checkinfo = dict_args["checkinfo"]
#
    print "start threads"
    str_result = ''
    str_result = get_info_remote(node, history, topsql, sysinfo, checkinfo, monitor_port)
    try:
        if mutex_lock.acquire():
            if sysinfo == '1':
                result_list.append(str_result)
            if checkinfo == '1':
                result_list.append(str_result)
            if topsql != '0':
                if '' == str_result:
                    mutex_lock.release()
                    return
                dict_result = json.loads(str_result)       # harvey
                result_list.extend(dict_result["SQLS"])    # harvey
            mutex_lock.release()
    except Exception as e:
        print  "get_datas_from_nodes:except......",e
        mutex_lock.release()

class KodeFunHTTPRequestHandler (BaseHTTPRequestHandler):
    # handle GET command
    def do_GET(self):
        global g_sysinfo
        global g_checkinfo
        global g_topsql
        global g_expand
        global g_hdfs
        global g_hdfs_who
        global g_topsql_who
        global g_topsql_info
        global g_hdfs_info
        global g_sys_info
        g_hdfs_who = 'false'
        g_topsql_who = 'false'
        g_hdfs = '0'
        g_sysinfo = '0'
        g_checkinfo = '0'
        g_topsql = '0'
        g_expand = 'false'
        g_topsql_who = 'false'
        g_hdfs_who = 'false'

        l_expand = ''
        l_hdfs = ''
        l_topsql = '0'
        l_twho = ''
        l_hwho = ''
        l_swho = ''
        l_history = ''
        l_sysinfo = ''
        l_checkinfo = ''
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
                elif key == 's_who':
                    l_swho = urldict['s_who']
                elif key == 'sysinfo':
                    l_sysinfo = urldict['sysinfo']
                elif key == 'checkinfo':
                    l_checkinfo = urldict['checkinfo']
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
            pid = myUtil.get_xcloud_pid()

            if l_sysinfo == '1' and  l_expand == 'false':
                collect_sys_cpu_mem_info(memdict, cpudict, pid)
                result = make_result_single(memdict, cpudict)
                result_list.append(result)

            if l_checkinfo == '1' and  l_expand == 'false':
                result = check_info.collect_sys_check_info()
                result_json = json.dumps(result)
                result_list.append(result_json)

            if l_topsql != '0':
                if l_twho == 'false' and l_expand == 'false':
                    print "i 'm g_topsql  who =false expand=false"
                    self.wfile.write(g_topsql_info)
                    return

                if l_twho == 'true' and l_expand == 'false':
                    dir = '../log'
                    sql_res = []
                    parsersql = parserSQL.ParserSQL(dir, myUtil.get_xcperf_port(), myUtil.get_xcloud_pid(),l_history,
                            int(l_topsql))
                    sql_res = parsersql.parser()
                    result_list.extend(sql_res)
            if l_expand == 'true':
                servers = get_zookerper_ip_and_port()
         
                if servers == None:
                    print "getZKserver err"
                    return

                host_name = socket.gethostname()   # eg.  host_name:xcloud9
                host_fqdn = socket.getfqdn(socket.gethostname())
                host_ip = socket.gethostbyname(host_fqdn)
                count = 1 
                node_monitorport, count = get_active_xcloudinfo_from_zookeer(servers, l_topsql)
                #pool = threadpool.ThreadPool(count)
                #dict_vars = {'node_monitorport':node_monitorport, 'l_history':l_history, 'l_topsql':l_topsql, 'l_sysinfo':l_sysinfo, 'l_checkinfo':l_checkinfo,'result_list':result_list}
                #func_var = [(None, dict_vars)]
                #requests = threadpool.makeRequests(get_datas_from_nodes, func_var) 
                #[pool.putRequest(req) for req in requests]
                #pool.wait()
                l_threads = []
                l_mutex_lock = threading.Lock()
                #dict_args =collections.OrderedDict()
                #dict_args["history"] = l_history
                #dict_args["topsql"] = l_topsql
                #dict_args["sysinfo"] = l_sysinfo
                #dict_args["checkinfo"] = l_checkinfo
                for node in node_monitorport:
                    #get_datas_from_nodes(node, monitor_port,history, topsql, sysinfo, checkinfo
                    #dict_args["node"] = node
                    #dict_args["monitor_port"] = node_monitorport[node]
                    t_monitor_port = node_monitorport[node]
                    if node.find('_') != -1:
                        node = node.split('_')[0]
                    t = threading.Thread(target=get_datas_from_nodes,args=(node, l_history, l_topsql, l_sysinfo, l_checkinfo,
                        t_monitor_port,result_list,l_mutex_lock))
                    t.start()
                    l_threads.append(t)
                for t in l_threads:
                    t.join()
                    print "wait children of threads over"

            if l_sysinfo == '1':
                #if l_expand == 'true':
                 #   result_list.append(g_sys_info)
                data = make_sysinfo_result_total(result_list, l_expand)
                print "sysinfo===1"
                self.wfile.write(data)
            if l_checkinfo == '1':
                data = make_checkinfo_result_total(result_list, l_expand)
                print "checkinfo===1"
                self.wfile.write(data)

            if l_topsql != '0':
                data = make_topsql_result_total(result_list, l_expand, l_topsql)
                result_list = []
                if data == '{"SQLS":[]}' or data == '':
                    if l_expand == 'true':
                        data = '{'\
                                '"SQLS":'\
                                '{'\
                                '"host":""'\
                                ','\
                                '"monitor_port":""' \
                                ',' \
                                '"xcloud_pid":""' \
                                ',' \
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
                print "expand===",l_expand,"and who ====",l_twho
                return

            if l_hdfs == '1':
                if 'false' == l_hwho:
                    print "l_hwho===",l_hwho
                    while 1:
                        sleep (1)
                        if g_hdfs_info != '':
                            self.wfile.write(g_hdfs_info)
                            return
                data = None
                spaceUsage = space_admin.SpaceUsage()
                ret, res = spaceUsage.get_xcloud_info_from_dhfs()
                if ret == -1:
                    data = res
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
                    #for r in res:
                data = json.dumps(res)
                self.wfile.write(data)
            if l_dbName != '' and l_userName != '':
                data = None
                spaceUsage = space_admin.SpaceUsage()
                ret, res = spaceUsage.do_space_usage_old(l_dbName, l_userName)
                if ret == -1:
                    data = res
                else:
                    data = json.dumps(res)
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
    port = myUtil.get_xcperf_port()
    server_address = (host_ip, port)
    #httpd = HTTPServer(server_address, KodeFunHTTPRequestHandler)
    httpd = ThreadedHTTPServer(server_address, KodeFunHTTPRequestHandler)
    print('http server is running...')
    pid_file = 'pid_mon'
    write_pid_to_file(pid_file)
    httpd.serve_forever()
    print "the end of  server_forver "

def requestsysinfo(request):
    global g_sys_info
    print "request====",request
    host_fqdn = socket.getfqdn(socket.gethostname())
    port = myUtil.get_xcperf_port()
    thread = threading.current_thread()
    print "sysinfo threading_ID====",thread.getName()
    sleep(5)
    while 1:
        conn = httplib.HTTPConnection(host_fqdn, port)
        conn.request('GET', 'index.html?sysinfo=1&expand=false&who=true')
        rsp = conn.getresponse()
        g_sys_info = rsp.read()
        print "g_sysinfo===", g_sys_info
        sleep(5)

def requesthdfs(request):
    global g_hdfs_requested
    global g_hdfs_info
    print "request====",request
    host_fqdn = socket.getfqdn(socket.gethostname())
    port = myUtil.get_xcperf_port()
    thread = threading.current_thread()
    print "hdfs threading_ID====",thread.getName()

    sleep(10)
    while 1:
        conn = httplib.HTTPConnection(host_fqdn, port)
        conn.request('GET', 'index.html?hdfs=1&who=true')
        rsp = conn.getresponse()
        g_hdfs_info = rsp.read()

        sleep(300)

def requesttopsql(request):
    global g_topsql_requested
    global g_topsql_info
    print "request====",request
    host_fqdn = socket.getfqdn(socket.gethostname())
    port = myUtil.get_xcperf_port()
    thread = threading.current_thread()
    print "topN threading_ID=====",thread.getName()
    sleep(5)
    while 1:
        redirectpfmonlog()
        conn = httplib.HTTPConnection(host_fqdn, port)
        url = 'index.html?top=10&history=24h&who=true&expand=false'
        conn.request('GET', url)
        rsp = conn.getresponse()
        g_topsql_info = rsp.read()
        print "g_topsql_info===",g_topsql_info
        sleep(6)

def redirectpfmonlog():
    filePath = '../log/pfmon.log'
    if os.path.exists(filePath):
        if os.path.getsize(filePath)/1024/1024 > 20:
	    os.system('>../log/pfmon.log')

def is_succeeded_xcloudd():
    while 1:
        redirectpfmonlog()
        sleep (5)
        pid_file = './pid'
        dir_struc = './dir_struc'
        if os.path.exists(pid_file):
            count = 1
            p = subprocess.Popen("ps -ef | grep `cat ./pid`|grep -v 'grep' |wc -l",shell=True,stdout=subprocess.PIPE)
            out = p.stdout.readlines()
            for line in out:
                count = line.strip()
            if int(count) == 0:
                if os.path.exists(dir_struc):
                    os.remove(dir_struc)
                subprocess.Popen("kill -9 `cat ./pid_mon`",shell=True,stdout=subprocess.PIPE)
                #if os.path.exists(pid_mon_file):

                 #os.remove(pid_mon_file)
        else:
            if os.path.exists(dir_struc):
                os.remove(dir_struc)
            subprocess.Popen("kill  -9 `cat ./pid_mon`",shell=True,stdout=subprocess.PIPE)
            #if os.path.exists(pid_mon_file):
             #   os.remove(pid_mon_file)



if __name__ == '__main__':
    threads = []
    t1 = threading.Thread(target=is_succeeded_xcloudd)
    threads.append(t1)
    t2 = threading.Thread(target=requesthdfs,args={'requesthd',})
    threads.append(t2)
    #t3 = threading.Thread(target=requesttopsql,args={'requesttopsql',})
    #threads.append(t3)
    #t4 = threading.Thread(target=requestsysinfo,args={'requestsysinfo',})
    #threads.append(t4)

    for t in threads:
        t.setDaemon(True)
        t.start()
    run()
    pass
