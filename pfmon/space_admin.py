#!/usr/bin/env python2
#coding: utf-8

import sys
import os
import re
import signal
import json
import subprocess
import collections
import commands
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

class SpaceUsage():
    def __init__(self):
        xcloud_ns = 'xcloud'
        cluster_name = 'xcloud'

        tree = ET.ElementTree(file='../conf/ds.xml')
        root = tree.getroot()
        for zk in root.findall('flag'):
            zk_xcloud = zk.find('name').text
            if zk_xcloud == 'xcloud_root_name':
                try:
                    xcloud_ns = zk.find('current').text
                except Exception:
                    xcloud_ns = zk.find('current').text
            if zk_xcloud == 'cluster_name':
                try:
                    cluster_name = zk.find('current').text
                except Exception:
                    cluster_name = zk.find('current').text

        self.xcloud_ns = 'xcloud'
        self.cluster_name = 'xcloud'

        #if os.path.exists('../upgrade-datadir-xcloud.sh') or os.path.exists('../xcloud-dataversion.sh'):   #2.2及之后的版本
        if os.path.exists('./dir_struc'):
            self.cluster_name = cluster_name
            dir_file = "dir_struc"
            struct = 'Old'
            with open(dir_file, 'r') as f:
                struct = f.readlines()[0].strip()
            if 'Old' != struct:#新的数据目录结构
                self.xcloud_ns = xcloud_ns
                self.cluster_name = cluster_name
	 	self.meta_path = '/%s/%s.meta' % (self.xcloud_ns,self.cluster_name)
		return
            self.meta_path = '/%s/%s.meta' % (self.xcloud_ns,self.cluster_name)#旧的数据目录结构
            return

        meta_tree = ET.ElementTree(file = '../conf/ds.xml') #2.2之前的数据目录结构
        meta_root = meta_tree.getroot()
        for meta in meta_root.findall('flag'):
            name = meta.find('name').text
            if name ==  'metadata_name':
                meta_path = meta.find('current').text
                self.meta_path = '/%s/%s.meta' %(self.xcloud_ns,meta_path)

    def do_space_usage_old(self, db_name, user_name):
        db_name = db_name.upper()
        user_name = user_name.upper()
        result = collections.OrderedDict()
        #ret, data_path = self.build_data_path(db_name, user_name)
        ret, data_path = self.build_data_path_by_dbname_username(db_name,user_name,db_path,db_batch)



        if -1 == ret:
            return -1, data_path

        du_output = os.popen("hadoop fs -du -s  %s" % (data_path)).readlines()

        if len(du_output) > 0 and du_output[0].find('No such file or directory') != -1:
            err =  'Error: Data dir is not found.[%s]' % data_path
            return -1, err

        dir_size     = 0
        bak_dir_size = 0
        items = re.split("\s\s+", du_output[0].strip())

        if len(items) == 3:
            dir_size     = items[0]
            bak_dir_size = items[1]

        space_output = os.popen("hadoop fs -count -q  %s" % (data_path)).readlines()

        if len(space_output) > 0 and space_output[0].find('No such file or directory') != -1:
            err =  'Error: Data dir is not found.[%s]' % data_path
            return -1, err

        quota                 = 0
        remaining_quota       = 0
        space_quota           = 0
        remaining_space_quota = 0
        dir_count             = 0
        file_count            = 0
        items = re.split("\s\s+", space_output[0].strip())

        if len(items) == 7:
            quota = items[0]
            remaining_quota = items[1]
            space_quota = items[2]
            remaining_space_quota = items[3]
            dir_count = items[4]
            file_count = items[5]

        result['UserName']            = user_name
        result['DataPath']            = data_path
        result['DirSize']             = dir_size
        result['TotalDirSize']        = bak_dir_size
        result['QuotaLimit']          = quota
        result['RemainingQuota']      = remaining_quota
        result['SpaceQuota']          = space_quota
        result['RemainingSpaceQuota'] = remaining_space_quota
        result['DirCount']            = dir_count
        result['FileCount']           = file_count
        return 0, result

    def do_space_usage(self, db_name,db_path, user_name, db_batch):
        #db_name = db_name.upper()
        #user_name = user_name.upper()
        result = collections.OrderedDict()
        #ret, data_path = self.build_data_path(db_name, user_name)
        ret, data_path = self.build_data_path_by_dbname_username(db_name,user_name,db_path,db_batch)
        if -1 == ret:
            return -1, data_path

        #data_path = '/%s/%s_%d/%s/' % (self.xcloud_ns,db_name,db_batch,user_name)
        # 查看用户目录总大小
        du_output = os.popen("hadoop fs -du -s  %s" % (data_path)).readlines()
        if len(du_output) > 0 and du_output[0].find('No such file or directory') != -1:
            err =  'Error: Data dir is not found.[%s]' % data_path
            return -1, err

        dir_size     = 0
        bak_dir_size = 0
        items = re.split("\s\s+", du_output[0].strip())
        if len(items) == 3:
            dir_size     = items[0]
            bak_dir_size = items[1]

        # 显示空间配额情况
        space_output = os.popen("hadoop fs -count -q  %s" % (data_path)).readlines()
        if len(space_output) > 0 and space_output[0].find('No such file or directory') != -1:
            err =  'Error: Data dir is not found.[%s]' % data_path
            return -1, err

        quota                 = 0
        remaining_quota       = 0
        space_quota           = 0
        remaining_space_quota = 0
        dir_count             = 0
        file_count            = 0
        items = re.split("\s\s+", space_output[0].strip())
        if len(items) == 7:
            quota = items[0]
            remaining_quota = items[1]
            space_quota = items[2]
            remaining_space_quota = items[3]
            dir_count = items[4]
            file_count = items[5]

        result['UserName']            = user_name
        result['DataPath']            = data_path
        result['DirSize']             = dir_size
        result['TotalDirSize']        = bak_dir_size
        result['QuotaLimit']          = quota
        result['RemainingQuota']      = remaining_quota
        result['SpaceQuota']          = space_quota
        result['RemainingSpaceQuota'] = remaining_space_quota
        result['DirCount']            = dir_count
        result['FileCount']           = file_count
        return 0, result

    def build_data_path(self, db_name,user_name):
        output = os.popen("hadoop fs -ls %s/%s/batch.* | awk -F '/' '{ print $5 }' | awk -F '.' '{ print $2 }'" % (self.meta_path,db_name)).read().strip()
        if not output.isdigit():
            err = 'Error: Invalid DB Name [%s]' % db_name
            return -1, err
        batchno = int(output)  # db batch no

        output = os.popen("hadoop fs -cat %s/%s/%s_%d.dat" % (self.meta_path,db_name,db_name,batchno)).read().strip()
        items = re.compile(r"^MDF1(.*)MDF1$").findall(output)
        if len(items) == 0:
            err = 'Error: DB file format is incorrect'
            return -1, err
        attrs = json.loads(items[0])
        db_batch = int(attrs['DB_ID_NEW'])  # db batch no

        output = os.popen("hadoop fs -ls %s/%s/%s/sys.batchNo/batch.* | awk -F '/' '{ print $7 }' | awk -F '.' '{ print $2 }'" % (self.meta_path,db_name,user_name)).read().strip()
        if not output.isdigit():
            err = 'Error: Invalid User Name [%s]' % user_name
            return -1, err
        batchno = int(output)  # user batch no

        output = os.popen("hadoop fs -cat %s/%s/%s/%s_%d.dat" % (self.meta_path,db_name,user_name,user_name,batchno)).read().strip()
        items = re.compile(r"^MDF1(.*)MDF1$").findall(output)
        if len(items) == 0:
            err = 'Error: User file format is incorrect'
            return -1, err
        attrs = json.loads(items[0])
        schema_batch = int(attrs['SH_ID_NEW'])  # user batch no

        res = '/%s/%s_%d/%s_%d/' % (self.xcloud_ns,db_name,db_batch,user_name,schema_batch)
        return 0, res

    def if_exist_dbname(self, db_name, db_path):
        output = os.popen("hadoop fs -ls %s/batch.* | awk -F '/' '{ print $5 }' | awk -F '.' '{ print $2 }'" %
                (db_path)).read().strip()
        if not output.isdigit():
            err = 'Error: Invalid DB Name [%s]' % db_name
            return -1, err
        batchno = int(output)  # db batch no

        output = os.popen("hadoop fs -cat %s/%s/%s_%d.dat" % (db_path,db_name,db_name,batchno)).read().strip()
        items = re.compile(r"^MDF1(.*)MDF1$").findall(output)
        if len(items) == 0:
            err = 'Error: DB file format is incorrect'
            return -1, err

        attrs = json.loads(items[0])
        db_batch = int(attrs['DB_ID_NEW'])  # db batch no
        return 0, db_batch

    def build_data_path_by_dbname_username(self, db_name, user_name,db_path, db_batch):
        output = os.popen("hadoop fs -ls %s/%s/sys.batchNo/batch.* | awk -F '/' '{ print $7 }' | awk -F '.' '{ print $2 }'" % (db_path,user_name)).read().strip()
        if not output.isdigit():
            err = 'Error: Invalid User Name [%s]' % user_name
            return -1, err
        batchno = int(output)  # user batch No

        output =  os.popen("hadoop fs -cat %s/%s/%s_%d.dat" % (db_path,user_name,user_name,batchno))
        output = output.read().strip()
        items = re.compile(r"^MDF1(.*)MDF1$").findall(output)
        if len(items) == 0:
            err = 'Error: User file format is incorrect'
            return -1, err
        attrs = json.loads(items[0])
        schema_batch = int(attrs['SH_ID_NEW'])  # user batch no
        #res = '/%s/%s_%d/%s_%d/' % (self.xcloud_ns,db_name,db_batch,user_name,schema_batch)
        res = '/%s/%s_%d/%s_%d/' % (self.xcloud_ns,db_name,db_batch,user_name,schema_batch)
        return 0, res

    def get_info_from_hdfs_by_dbname(self, db_name, db_path, db_batch):
        db_name = db_name.upper()
        #ret, batch = self.if_exist_dbname(db_name, db_path)
        #if -1 == ret:
            #return -1, "error"

        result = collections.OrderedDict()
        result_user = collections.OrderedDict()
        result_db = collections.OrderedDict()
        result_list = []
        space_db = os.popen("hadoop fs -count -q /%s/%s_%d" %(self.xcloud_ns, db_name, db_batch)).read().strip()
        if '' == space_db:
            return -1, "error"
        space_db = ' '.join(space_db.split())
        quota  = space_db.split()[0]
        remaining_quota = space_db.split()[1]
        space_quota = space_db.split()[2]
        remaining_space_quota = space_db.split()[3]
        dir_count = space_db.split()[4]
        file_count = space_db.split()[5]
        content_size = space_db.split()[6]
        file_name = space_db.split()[7]
        result_db['db_name'] = db_name
        result_db['quota'] = quota
        result_db['remaining_quota'] = remaining_quota
        result_db['space_quota'] = space_quota
        result_db['remaining_space_quota'] = remaining_space_quota
        result_db['dir_count'] = dir_count
        result_db['file_count'] = file_count
        result_db['content_size'] = content_size
        result_db['db_dir'] = file_name 
        output = os.popen("hadoop fs -ls /%s/%s_%d" %(self.xcloud_ns, db_name, db_batch)).readlines()
        count = 0
        for line in output:
            if 0 == count:
                count = count + 1
                continue
            line  = ' '.join(line.split())
            user_name = line.split()[7]
            user_name = commands.getoutput("echo %s | awk -F '/' '{ print $4 }'" % (user_name))
            if user_name == "unlock":
                break
            user = ""
            if -1 != user_name.find('_'):
                number = user_name.count('_')
                count = 0
                while 1:
                    result_one = {}
                    if number  == count + 1:
                        user = user + user_name.split('_')[count]
                        break
                    user = user + user_name.split('_')[count] + "_"
                    count = count + 1
            result_one = {}
            ret, result_one = self.do_space_usage(db_name, db_path, user, db_batch)
            if -1 == ret:
                #result_one = {}
                #result_list.append(result_one)
                continue
            #result_user[user] = result_one
            result_list.append(result_one)
            result_one = {}
        #result[db_name] = result_db
        result_db['users'] = result_list
        #result[] = result_db
        return 0, result_db

    def get_xcloud_info_from_dhfs(self):
        count = 0
        db_list = []
        xcloud_dict = collections.OrderedDict()
        db_result = collections.OrderedDict()
        #db_lines = os.popen("hadoop fs -ls /%s | grep 'meta' " % (self.meta_path)).readlines()
        #db_lines = os.popen("hadoop fs -ls /%s " % (self.meta_path)).readlines()
        #for db_line in db_lines:
            #count = 0
            #db_line = ' '.join(db_line.split())
            #db_line = db_line.split()[7]
            #print "db_Line======",db_line
            #db_name_paths = os.popen("hadoop fs -ls %s" %(db_line)).readlines()
        db_name_paths = os.popen("hadoop fs -ls %s" %(self.meta_path)).readlines()
        for db_name_path in db_name_paths:
            if 0 == count:
                count = count + 1
                continue
            db_name_path = ' '.join(db_name_path.split())
            db_name_path = db_name_path.split()[7]
            batch = os.popen("hadoop fs -ls %s/batch.* | awk -F '/' '{ print $5 }' | awk -F '.' '{ print $2 }'" %
                    (db_name_path)).read().strip()
            if not batch.isdigit():
                continue
            batchno = int(batch)
            db_name =  commands.getoutput("echo %s | awk -F '/' '{ print $4 }'" % (db_name_path))
            output = os.popen("hadoop fs -cat %s/%s_%d.dat" %(db_name_path, db_name,batchno)).read().strip()
            items = re.compile(r"^MDF1(.*)MDF1$").findall(output)
            if len(items) == 0:
                continue
            attrs = json.loads(items[0])
            db_batch = int(attrs['DB_ID_NEW'])
            ret, res = self.get_info_from_hdfs_by_dbname(db_name, db_name_path, db_batch)
            if -1 == ret:
                continue
            db_list.append(res)
            res = {}

        #db_result['xcloud'] = db_list
        #space_xcloud = os.popen("hadoop fs -count -q /%s" %(self.xcloud_ns)).read().strip()
        space_xcloud = os.popen("hadoop fs -count -q %s" %(self.meta_path)).read().strip()
        if '' == space_xcloud:
            return -1, "no data from /%s"%(self.meta_path)
        space_xcloud = ' '.join(space_xcloud.split())
        quota  = space_xcloud.split()[0]
        remaining_quota = space_xcloud.split()[1]
        space_quota = space_xcloud.split()[2]
        remaining_space_quota = space_xcloud.split()[3]
        dir_count = space_xcloud.split()[4]
        file_count = space_xcloud.split()[5]
        content_size = space_xcloud.split()[6]
        file_name = space_xcloud.split()[7]
        xcloud_dict['quota'] = quota
        xcloud_dict['remaining_quota'] = remaining_quota
        xcloud_dict['space_quota'] = space_quota
        xcloud_dict['remaining_space_quota'] = remaining_space_quota
        xcloud_dict['dir_count'] = dir_count
        xcloud_dict['file_count'] = file_count
        xcloud_dict['content_size'] = content_size
        xcloud_dict['xcloud_dir'] = file_name
        db_result['xcloud_hdfs_info'] = xcloud_dict
        db_result['xcloud'] = db_list
        return 0, db_result
