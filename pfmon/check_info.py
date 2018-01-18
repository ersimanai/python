#!/usr/bin/env python
#coding: utf-8

from time import sleep
import os
import commands
import string
import psutil
import socket


class CollectSysInfo:
    def __init__(self):
        pass

    """
    收集内存信息
    """
    def collect_sys_mem_info(self):
        memdict = {'MemTotal': 0, 'MemFree': 0}
        with open("/proc/meminfo") as mem_file:
            for line in mem_file:
                name = line.split(':')[0]
                if name == 'MemTotal':
                    memdict['MemTotal'] = string.atoi(line.split(':')[1].split()[0])
                elif name == 'MemFree':
                    memdict['MemFree'] = string.atoi(line.split(':')[1].split()[0])
                else:
                    continue
        memdict['MemPercent'] = psutil.virtual_memory().percent
        return memdict


    """
    收集CPU信息
    """
    def collect_sys_cpu_info(self):
        cpudict = {}
        cpudict['cpuCore'] = psutil.cpu_count()
        cpudict['cpuUsed'] = psutil.cpu_percent()
        return cpudict


    """
    收集负载信息1分钟、五分钟、15分钟
    """
    def collect_sys_load_info(self):
        loadavgdict = {}
        load_file = open("/proc/loadavg")
        content = load_file.read().split()
        load_file.close()
        loadavgdict["load1"] = content[0]
        loadavgdict["load5"] = content[1]
        loadavgdict["load15"]= content[2]
        return loadavgdict

    """
    收集进程总数
    """
    def collect_sys_proc_info(self):
        processdict = {}
        pids = []
        for subdir in os.listdir('/proc'):
            if subdir.isdigit():
                pids.append(subdir)
        processdict["count"] = len(pids)
        return processdict

    """
    收集网卡信息
    """
    def collect_sys_net_info(self):
        netdict = {}
        with open("/proc/net/dev") as net_file:
            for line in net_file:
                if line.find(":") < 0:
                    continue
                card_name = line.split(":")[0].strip()
                if card_name.startswith("eth") or card_name.startswith("em") or card_name.startswith("lo"):
                    line_fields = line.split(":")[1].lstrip().split()
                    netdict["InBytes"] = string.atoi(line_fields[0])  # Byte
                    netdict["InPackets"] = string.atoi(line_fields[1])
                    netdict["InErrors"] = string.atoi(line_fields[2])
                    netdict["InDrops"] = string.atoi(line_fields[3])
                    netdict["OutBytes"] = string.atoi(line_fields[8])  # Byte
                    netdict["OutPackets"] = string.atoi(line_fields[9])
                    netdict["OutErrors"] = string.atoi(line_fields[10])
                    netdict["OutDrops"] = string.atoi(line_fields[11])
        return netdict

    """
    计算网卡指标差值
    """
    def cal_sys_net_info(self):
        netdict = {}
        net_info = {}
        net_last_info = {}
        net_last_info = self.collect_sys_net_info()
        sleep(10)
        net_info = self.collect_sys_net_info()
        netdict["InBytes"] = net_info["InBytes"] - net_last_info["InBytes"]
        netdict["InPackets"] = net_info["InPackets"] - net_last_info["InPackets"]
        netdict["InErrors"] = net_info["InErrors"] - net_last_info["InErrors"]
        netdict["InDrops"] = net_info["InDrops"] - net_last_info["InDrops"]
        netdict["OutBytes"] = net_info["OutBytes"] - net_last_info["OutBytes"]
        netdict["OutPackets"] = net_info["OutPackets"] - net_last_info["OutPackets"]
        netdict["OutErrors"] = net_info["OutErrors"] - net_last_info["OutErrors"]
        netdict["OutDrops"] = net_info["OutDrops"] - net_last_info["OutDrops"]
        return netdict
    """
    收集tcp指标
    """
    def collect_sys_tcp_info(self):
        tcpdict = {}
        is_title = True
        with open("/proc/net/snmp") as tcp_file:
            for line in tcp_file:
                protocol_name = line.split(":")[0].strip()
                if protocol_name == "Tcp":
                    if is_title:
                        is_title = False
                        continue
                    else:
                        line_fields = line.split(":")[1].lstrip().split()
                        tcpdict["ActiveOpens"] = string.atoi(line_fields[4])
                        tcpdict["PassiveOpens"] = string.atoi(line_fields[5])
                        tcpdict["InSegs"] = string.atoi(line_fields[9])
                        tcpdict["OutSegs"] = string.atoi(line_fields[10])
                        tcpdict["RetransSegs"] = string.atoi(line_fields[11])
                        tcpdict["CurrEstab"] = string.atoi(line_fields[8])
                        break
        return tcpdict

    """
    计算tcp指标差值
    """
    def cal_sys_tcp_info(self):
        tcpdict = {}
        last_tcpdict = {}
        cur_tcpdict = {}
        last_tcpdict = self.collect_sys_tcp_info()
        sleep(10)
        cur_tcpdict = self.collect_sys_tcp_info()
        tcpdict["ActiveOpens"] = cur_tcpdict["ActiveOpens"] - last_tcpdict["ActiveOpens"]
        tcpdict["PassiveOpens"] = cur_tcpdict["PassiveOpens"] - last_tcpdict["PassiveOpens"]
        tcpdict["InSegs"] = cur_tcpdict["InSegs"] - last_tcpdict["InSegs"]
        tcpdict["OutSegs"] = cur_tcpdict["OutSegs"] - last_tcpdict["OutSegs"]
        tcpdict["RetransSegs"] = cur_tcpdict["RetransSegs"] - last_tcpdict["RetransSegs"]
        tcpdict["CurrEstab"] = cur_tcpdict["CurrEstab"] - last_tcpdict["CurrEstab"]
        return tcpdict
    def collect_sys_disk_info_by_psutil(self):
        mountlist = []
        diskinfo = psutil.disk_partitions()
        for disk in diskinfo:
            mountdict = {}
            mount = psutil.disk_usage(disk[1]) 
            mountdict['FileSystem'] = disk[0]
            mountdict['Name'] = disk[1]
            mountdict['Size'] = str(long(mount[0]) / 1024)
            mountdict['Used'] = str(long(mount[1]) / 1024)
            mountdict['Avail'] = str(long(mount[2]) / 1024)
            mountdict['UsedPercent'] = mount[3]
            mountlist.append(mountdict)
        return mountlist
    """
    收集IO信息
    """
    def collect_sys_disk_info(self):
        mountlist = []
        mount = commands.getoutput('df -k')
        file_reader = mount.split("\n")

        for line in file_reader:
            if (line.startswith("Filesystem") or line.startswith("df")):
                continue
            else:
                mountdict = {}
                mountdict["Name"] = line.split()[5]
                mountdict["Size"] = line.split()[1]
                mountdict["Used"] = line.split()[2]
                mountdict["Avail"] = line.split()[3]
                mountdict["UsedPercent"] = line.split()[4]
                mountdict["FileSystem"] = line.split()[0]
                mountlist.append(mountdict)
        return mountlist


"""
诊断工具
"""
def collect_sys_check_info():
    collect = CollectSysInfo()
    result = {}
    # 收集内存
    memDict = collect.collect_sys_mem_info()
    # 收集CPU
    cpuDict = collect.collect_sys_cpu_info()
    # 收集负载
    loadAvgDict = collect.collect_sys_load_info()
    # 收集磁盘IO
    #diskList = collect.collect_sys_disk_info()
    diskList = collect.collect_sys_disk_info_by_psutil()
    # 收集总进程数
    processDict = collect.collect_sys_proc_info()
    # 收集网卡指标
    netDict = collect.collect_sys_net_info()
    # 收集TCP指标
    tcpDict = collect.collect_sys_tcp_info()
    # 汇总
    result['host'] = socket.gethostname() + ":" + socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    result['memory'] = memDict
    result['cpu'] = cpuDict
    result['load_avg'] = loadAvgDict
    result['mount'] = diskList
    result['process'] = processDict
    result['net'] = netDict
    result['tcp'] = tcpDict
    return result


if __name__ == '__main__':
    info = collect_sys_check_info()
    print info
