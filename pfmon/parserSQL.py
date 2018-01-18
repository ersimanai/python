#!/usr/bin/env python
#coding: utf-8

import os
import socket
import datetime, time
import re


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
    def __init__(self, dirPath, monitor_port, xcloud_pid, rectime="24h", topN=10):
        self._dir   = dirPath
        self._fileList = []   # 保存当前未处理完成的新文件
        self._time  = rectime
        self._stopTime = datetime.datetime.now()
        self._startTime = datetime.datetime.now()
        self._count = topN
        self._sqls  = []   # add dict sql, count+1
        self._monitorPort = monitor_port
        self._xcloud_pid = xcloud_pid
        self._sqlDict = {}
        self._flag_isNeedNextLine = False

    def initReCompile(self):
        #self.standard_pattern = re.compile(standard_regex)
        #self.queryId_pattern  = re.compile(queryId_regex)
        #self.stmt_pattern     = re.compile(stmt_regex)
        #self.startTime_pattern= re.compile(startTime_regex)
        #self.stopTime_pattern = re.compile(stopTime_regex)
        self.usedTime_pattern = re.compile(end_regex)
        #self.runTime_pattern  = re.compile(runTime_regex)


    '''
    Only need use this interface
    '''
    def parser(self):
        self.parserDate()
        self.findFiles()
        self.initReCompile()
        self.findSqlNormal()
        #self._sqls.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))   # 降序
        if len(self._sqls) == self._count+1:#改成先降序再取出N条sql
            self._sqls = self._sqls[:-1]
        name = socket.getfqdn(socket.gethostname())
        addr = socket.gethostbyname(name)
        rank = 1
        for sql in self._sqls:
            sql['rank'] = rank
            sql['host'] = addr
            sql['monitor_port'] = str(self._monitorPort)
            sql['xcloud_pid'] = str(self._xcloud_pid)
            # sql['runTime'] = str(sql['runTime']) + 'ms'
            rank += 1
        #sqlsDict = {'SQLS':self._sqls}
        #sqlsJson = json.dumps(sqlsDict)
        return self._sqls

    '''
    compare fileList,accoring to file ctime, DSC
    '''
    def compareFile(self, file1, file2):
        stat_file1 = os.stat(self._dir + "/" + file1)
        stat_file2 = os.stat(self._dir + "/" + file2)
        if stat_file1.st_ctime > stat_file2.st_ctime:
            return 1
        elif stat_file1.st_ctime < stat_file2.st_ctime:
            return -1
        else:
            return 0

    def parserDate(self):
        tmpTime = '-'
        tmpTime += self._time[:-1]
        self._stopTime = datetime.datetime.now()
        if self._time.find('d') != -1:
            self._startTime = datetime.datetime.now() + datetime.timedelta(days=int(tmpTime))
        elif self._time.find('h') != -1:
            self._startTime = datetime.datetime.now() + datetime.timedelta(hours=int(tmpTime))
        elif self._time.find('m') != -1:
            self._startTime = datetime.datetime.now() + datetime.timedelta(minutes=int(tmpTime))
        elif self._time.find('s') != -1:
            self._startTime = datetime.datetime.now() + datetime.timedelta(seconds=int(tmpTime))
        else:
            pass


    '''
    find STMT files
    '''
    def findFiles(self):
        # 查找，遍历所有文件
        files = []
        iterms = os.listdir(self._dir)
        for iterm in iterms:
            # 不处理压缩文件
            if iterm.find("STMT_.log") > -1 and iterm.find("tar.bz2") == -1:
                files.append(iterm)
                files.sort(self.compareFile)  # 升序，新文件在后

        # 保存所有符合时间段内的文件, files已按时间升序排序
        for afile in files:
            stat_afile = os.stat(self._dir + "/" + afile)
            if datetime.datetime.fromtimestamp(stat_afile.st_ctime) > self._startTime:
                self._fileList.append(afile)
        # 去重
        tmpList = sorted(set(self._fileList), key=self._fileList.index)
        self._fileList = tmpList
        # 因为行云启动时会创建2个STMT文件,第二个不是xcloud的却是最新
        pid_last_file = self._fileList[-1].split(".")[-1]
        if pid_last_file != str(self._xcloud_pid):
            self._fileList.remove(self._fileList[-1])

    def processNewLine(self, line1):
        #print "NewLine:",line1
        # 标准行的开始:  S1123 15:08:25.905017  5886 logging.h:88] query_id[590c10ac:5a1673e9:0:0], stmt[
        # 标准行的开始:  S1123 15:08:25.905017  5886 logging.h:89] query_id[590c10ac:5a1673e9:0:0], stmt[
        stmtPos = line1.find('], stmt[')
        inflightPos = line1.rfind('], is_inflight[')
        runTimePos = line1.rfind('], run_time[')
        useTimePos = line1.rfind(']. Query Used Time Total [')
        startTimePos = line1.rfind('], start_time[')
        stopTimePos = line1.rfind('], stop_time[')
        if line1[27:39] == ' logging.h:8' and line1[40:51] == '] query_id[' and stmtPos != -1:
            # 标准行的结束，即此行为完整内容
            if inflightPos != -1 and runTimePos != -1 and useTimePos != -1:
                self._sqlDict["queryID"] = line1[51:stmtPos]
                self._sqlDict["startTime"] = line1[startTimePos+14:stopTimePos]
                self._sqlDict["stopTime"] = line1[stopTimePos+13:runTimePos]
                self._sqlDict["sql"] = line1[stmtPos+8:inflightPos]
                self._sqlDict["runTime"] = line1[runTimePos+12:useTimePos-4]
                # usedTime使用正则，因为其后内容可变,且此时子串很短
                usedTime  = self.usedTime_pattern.search(line1[useTimePos:]).group()
                self._sqlDict["usedTime"] = usedTime.split(']')[0][25:]
                # 处理完整行
                self.processLine()
                self._sqlDict = {}
            elif inflightPos == -1 and runTimePos == -1 and useTimePos == -1:
                # 此时有开头没结尾,保存,取下一行拼装
                # 当前行有回车
                if line1.find('\r\n') != -1:  # windows回车
                    #print "this line contain \\r\\n",line1
                    line1 = line1.replace('\r\n', ' ')
                self._sqlDict["queryID"] = line1[51:stmtPos]
                self._sqlDict["sql"] = line1[stmtPos+8:]
                self._flag_isNeedNextLine = True
            else:
                pass
                #print "Not standard format: ", line1    # 不会出现，标准行的结束内容一定会同时出现在一行里
        # 非标准行的开始
        else:
            pass
            #print "Ignore this line: ", line1

    def processNextLine(self, line1):
        #print "NextLine:",line1
        if line1[27:39] == ' logging.h:8':
            # 处理一条日志被glog分两次打印
            #print "this line is glog #print multiple times:",line1
            line1 = line1[41:]
        # 处理多行显示
        # 标准行的结束，即此行为止为完整内容
        inflightPos = line1.rfind('], is_inflight[')
        runTimePos = line1.rfind('], run_time[')
        useTimePos = line1.rfind(']. Query Used Time Total [')
        startTimePos = line1.rfind('], start_time[')
        stopTimePos = line1.rfind('], stop_time[')
        if inflightPos != -1 and runTimePos != -1 and useTimePos != -1:
            self._sqlDict["startTime"] = line1[startTimePos+14:stopTimePos]
            self._sqlDict["stopTime"] = line1[stopTimePos+13:runTimePos]
            self._sqlDict["sql"] = self._sqlDict["sql"] + line1[:inflightPos]
            self._sqlDict["runTime"] = line1[runTimePos+12:useTimePos-4]
            # usedTime使用正则，因为其后内容可变,且此时子串很短
            usedTime  = self.usedTime_pattern.search(line1[useTimePos:]).group()
            self._sqlDict["usedTime"] = usedTime.split(']')[0][25:]
            # 处理完整行
            self.processLine()
            self._flag_isNeedNextLine = False
            self._sqlDict = {}
        elif line1.rfind('\r\n') != -1:  # windows回车 继续取下一行
            #print "this line contain \\r\\n",line1
            line1 = line1.replace('\r\n', ' ')
            self._sqlDict["sql"] = self._sqlDict["sql"] + line1
            self._flag_isNeedNextLine = True
        else:
            ##print "Not standard format: ", line1
            #print "this line maybe have problem: ",line1
            if self._sqlDict.has_key("sql"):
                self._sqlDict["sql"] = self._sqlDict["sql"] + line1
            else:
                self._sqlDict["sql"] = line1
            self._flag_isNeedNextLine = True

    '''
    按行查找格式符合的行交由processLine处理,每次调用都遍历所有文件的内容
    '''
    def findSqlNormal(self):
        for oneFile in self._fileList:
            tfile = open(self._dir + "/" + oneFile)
            while True:
                lines = tfile.readlines(50000)
                if not lines:
                    break
                for line1 in lines:
                    if self._flag_isNeedNextLine is False:
                        self.processNewLine(line1)
                    else:
                        self.processNextLine(line1)

    def processLine(self):
        stopTimeStr = self._sqlDict["stopTime"]  # ', stop_time[Fri Feb 17 22:03:58 2017]'
        stopTime = datetime.datetime.strptime(stopTimeStr, '%a %b %d %H:%M:%S %Y')
        if self._startTime < stopTime and stopTime < self._stopTime:  # sql在指定时间范围内
            self.addCmpSQL1()

    def isCorrect(self, srcStr):
        if self.standard_pattern.search(srcStr) is not None:
            return True
        else:
            return False

    '''
    add sql to self._sqls,  and compare, 正则方式，大字符串会很慢，同时CPU暴涨
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
        sqlDict = {'queryID':queryId, 'startTime':startTime, 'stopTime':stopTime, 'usedTime':usedTime,
                'runTime':runTime, 'sql':stmt, 'host':"", 'rank':'0'}     # harvey
        #print "-------",sqlDict,"------"
        if len(self._sqls) < self._count+1:  # _sqls存放count+1个元素,+1元素用于存放新的元素，以便排序
            self._sqls.append(sqlDict)    # 此时len(_sqls) = _count+1
        else:
            self._sqls[self._count] = sqlDict    # count+1 是最小的runTime
            self._sqls.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))   # 降序

    '''
    add sql to self._sqls,  and compare, 切割方式，更快
    '''
    def addCmpSQL1(self):
        # Mon Jul  3 17:39:58 2017  ==> 2017-07-03 18:05:06  ; str=>ptime=>str
        tmpStartTime = time.strptime(self._sqlDict["startTime"], '%a %b %d %H:%M:%S %Y')
        tmpStopTime = time.strptime(self._sqlDict["stopTime"], '%a %b %d %H:%M:%S %Y')
        startTime = time.strftime('%Y-%m-%d %H:%M:%S', tmpStartTime)
        stopTime = time.strftime('%Y-%m-%d %H:%M:%S', tmpStopTime)
        self._sqlDict["startTime"] = startTime
        self._sqlDict["stopTime"] = stopTime
        self._sqlDict["rank"] = '0'
        sqlDict = self._sqlDict
        #print "-------",sqlDict,"------"
        if len(self._sqls) < self._count+1:  # _sqls存放count+1个元素,+1元素用于存放新的元素，以便排序
            self._sqls.append(sqlDict)    # 此时len(_sqls) = _count+1
        else:
            self._sqls[self._count] = sqlDict    # count+1 是最小的runTime
            #self._sqls.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))   # 降序
            #排序 降序
        self.insertSort()
        #self._sqls.sort(lambda a,b:int(b['runTime'])-int(a['runTime']))   # 降序

    # 插入排序 降序
    def insertSort(self):
        for i in range(1, len(self._sqls)):
            if self._sqls[i-1] < self._sqls[i]:
                temp = self._sqls[i]
                index = i
                while index > 0 and (int(self._sqls[index-1]['runTime']) < int(temp['runTime'])):
                    self._sqls[index] = self._sqls[index-1]
                    index -= 1
                self._sqls[index] = temp




if __name__ == '__main__':
    ps = ParserSQL('../log/', '10012', '19296', '24h', 10)
    while True:
        ps.parser()
        time.sleep(20)
