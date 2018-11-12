from __future__ import print_function

import subprocess

import pyarrow as pa

class hdfs_engine:
  def __init__(self, host, port, username = None, driver = 'libhdfs'):
    self.host = host
    self.port = port
    self.username = username
    self.driver = driver
    
    self.pa = pa.hdfs.connect(self.host, self.port, self.username, self.driver)
    
  ##need work
  def connect(self):
    """connect to hdfs"""
    if self.pa:
       self.disconnect()
    try:
      self.pa = pa.hdfs.connect(self.host, self.port, self.username, self.driver)
    except:
      
  ##need work
  def disconnect(self):
    """disconnect to hdfs"""
    self.pa.close()
    self.pa = None
    
  def exists(self, datadir):
    """hdfs folder exists"""
    return self.pa.exists(datadir)

  def get_folderlist(self, datadir):
    """collect folder name list under the hdfs datadir"""
    files = self.pa.ls(datadir)
    folder_name_list = [ line.split("/")[-1] for line in files if len(line.split("/")) ]
    return folder_name_list

  def get_listcsv(self, datadir):
    """collect csv file name list under the hdfs datadir"""
    files = self.pa.ls(datadir)
    file_name_list = [ line.split("/")[-1].split(".")[0] for line in files if len(line) > 4 and line[-4:] == '.csv' ]
    return file_name_list

  def create_folder(self, datadir):
    """create hdfs folder"""
    if self.exists(datadir):
      print("Warning: " + datadir + " exists.  Cannot create the folder.")
      return False
    
    self.pa.mkdir(datadir)
    if not self.exists(datadir):
      print("Warning: Failed to create " + datadir)
      return False
    
    return True

  def rename(self, datadir, newdatadir):
    """rename hdfs folder"""
    self.pa.rename(datadir, newdatadir)
    
    if self.exists(datadir) or not self.exists(newdatadir):
      print("Warning: Failed to move " + datadir + " to " + newdatadir)
      return False
      
    return True



  def rm(self, datapath, recursive = False):
    """remove hdfs file/folder"""
    self.pa.delete(datapath, recursive)
    
    if self.exists(datapath):
      print("Warning: Failed to remove " + datapath)
      return False
      
    return True