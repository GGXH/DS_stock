import _init_paths
import util.status.messager as msg

class hdfs_engine:
  def __init__(self):
   self._msg = msg()
  
  @property
  def msg(self):
   return self._msg
   
  @msg.setter
  def msg(self, msg):
   if type(msg) is not util.status.messager:
     raise TypeError('Wrong messager set to hdfs engine')
   self._msg = msg
    
  def exists(self, datadir):
    """check if hdfs folder exists"""
    return False

  def get_folderlist(self, datadir):
   """return folder list"""
    return []

  def get_listfile(self, datadir):
    """collect csv file name list under the hdfs datadir"""
    return []

  def create_folder(self, datadir):
    """create hdfs folder"""
    return False

  def rename(self, datadir, newdatadir):
    """rename hdfs folder"""
    return False

  def rm(self, datapath, recursive = False):
    """remove hdfs file/folder"""
    return False
    
  def _rm_file(self, filepath):
    """remove hdfs file"""
    return False
    
  def _rm_folder(self, folderpath):
    """remove hdfs folder"""
    return False