import subprocess

import hdfs_engine

class raw_hdfs_engine(hdfs_engine):
  def __init__(self):
    hdfs_engine.__init__(self)
 
  def exists(self, datadir):
    """check if hdfs folder exists"""
    return subprocess.call(["hdfs", "dfs", "-test", "-e", datadir]) == 0

  def list_folder(self, datadir):
    """collect file name list under the hdfs datadir"""
    if not self.folder_exists(datadir)
      return []
      
    try:
      files = subprocess.check_output(["hdfs", "dfs", "-ls", datadir])
    except subprocess.CalledProcessError as cpe:
      self.msg.clear()
      self.msg.code = cpe.code
      self.msg.message = cpe.output
      raise self.msg
    else:
      folder_name_list = []
      for line in files.split():
        ll = line.split("/")
        if len(ll) > 1:
          folder_name_list.append(ll[-1])
    return folder_name_list

  def create_folder(self, datadir):
    """create hdfs folder"""
    if self.exists(datadir):
      self.msg.clear()
      self.msg.code = -1
      self.msg.message = "[Error]: " + datadir + " exists and Failed to create"
      raise self.msg
    
    rcode = subprocess.call(["hdfs", "dfs", "-mkdir", datadir])
    if rcode != 0:
      self.msg.clear()
      self.msg.code = rcode
      self.msg.message = "[Error]: Failed to create hdfs folder " + datadir
      raise self.msg


  def rename(self, datadir, newdatadir):
    """rename hdfs folder"""
    rcode = subprocess.call(["hdfs", "dfs", "-mv", datadir, newdatadir])
    if rcode != 0:
      self.msg.clear()
      self.msg.code = rcode
      self.msg.message = "[Error]: Failed to rename hdfs dolder " + datadir + " to " + newdatadir
      raise self.msg


  def rm(self, datapath):
    """remove hdfs file/folder"""
    try:
      self._rm(datapath)
    except util.status.messager as msg:
      raise msg


  def _rm(self, filepath):
    """remove hdfs file"""
    if not self.exists(filepath):
      return
    rcode = subprocess.call(["hdfs", "dfs", "-rm", "-r", datapath])
    if rcode != 0:
      self.msg.clear()
      self.msg.code = rcode
      self.msg.message = "[Error]: Failed to remove hdfs file " + datapath
      raise self.msg


  def _leave_safe_mode(self):
    """leave safe mode"""
    rcode = subprocess.cal(["hdfs", "dfs", "-dfsadmin", "leave"])
    if rcode != 0:
      self.msg.clear()
      self.msg.code = rcode
      self.msg.message = "[Error]: Failed to turn off safemode of hdfs"
      raise self.msg
