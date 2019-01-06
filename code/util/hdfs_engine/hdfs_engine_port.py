import _init_paths
import util.status.messager as msg

class hdfs_engine:
  def __init__(self, host = None, port = None, username = None, driver = None):
   self._host = host
   self._port = port
   self._username = username
   self._driver = driver
   self._msg = msg()
  
  @property
  def driver(self):
   return self._driver
   
  @driver.setter
  def driver(self, driver):
   self._driver = driver 
  
  @property
  def host(self):
   return self._host
   
  @host.setter
  def host(self, host):
   self._host = host
   
  @property
  def port(self):
   return self._port
   
  @port.setter
  def port(self, port):
   self._port = port
   
  @property
  def username(self):
   return self._username
   
  @username.setter
  def username(self, username):
   self._username = username
    
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