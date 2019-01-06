import subprocess

import hdfs_engine

class raw_hdfs_engine(hdfs_engine):
  def __init__(self):
    hdfs_engine.__init__(self)
 
  def exists(self, datadir):
    """check if hdfs folder exists"""
    return subprocess.call(["hadoop", "fs", "-test", "-d", datadir]) == 0

  def list_folder(self, datadir):
    """collect file name list under the hdfs datadir"""
    if not self.folder_exists(datadir)
      return []
      
    try:
      files = subprocess.check_output(["hadoop", "fs", "-ls", datadir])
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
    
    rcode = subprocess.call(["hadoop", "fs", "-mkdir", datadir])
    if rcode != 0:
      self.msg.clear()
      self.msg.code = rcode
      self.msg.message = "[Error]: Failed to create hdfs folder " + datadir
      raise self.msg


  def rename(self, datadir, newdatadir):
    """rename hdfs folder"""
    rcode = subprocess.call(["hadoop", "fs", "-mv", datadir, newdatadir])
    if rcode != 0:
      self.msg.clear()
      self.msg.code = rcode
      self.msg.message = "[Error]: Failed to rename hdfs dolder " + datadir + " to " + newdatadir
      raise self.msg


  def rm(self, datapath, recursive = False):
    """remove hdfs file/folder"""
    return False   


def recreate_hdfs_folder(datadir):
 """create hdfs folder"""
 rcode = subprocess.call(["hadoop", "fs", "-test", "-d", datadir])
 if rcode == 0:
  rcode = rm_hdfs_folder(datadir)
  if rcode != 0:
   return rcode
 ##
 rcode = subprocess.call(["hadoop", "fs", "-mkdir", datadir])
 if rcode != 0:
  print "Failed to create hdfs folder" + datadir
 return rcode 



def rename_hdfs_folder(datadir, newdatadir):
 """rename hdfs folder"""
 rcode = subprocess.call(["hadoop", "fs", "-mv", datadir, newdatadir])
 if rcode != 0:
  print "Failed to rename hdfs folder " + datadir + " to " + newdatadir
 return rcode 



def rm_hdfs_file(datapath):
 """remove hdfs file"""
 rcode = subprocess.call(["hadoop", "fs", "-rm", datapath])
 if rcode != 0:
  print "Failed to remove hdfs file " + datapath
 return rcode



def rm_hdfs_folder(datadir):
 """remove hdfs folder"""
 rcode = subprocess.call(["hadoop", "fs", "-test", "-d", datadir])
 if rcode == 0:
  rcode = subprocess.call(["hadoop", "fs", "-rm", "-r", datadir])
  if rcode != 0:
   print "Failed to remove hdfs folder " + datadir
 else:
  rcode = 0
 return rcode
