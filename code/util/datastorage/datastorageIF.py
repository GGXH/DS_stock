import util.status.messager as msg

class datastorageIF:
  def __init__(self):
    self._msg = msg()
   
  def createOrGetEngine(self):
    self._msg.code = -1
    self._msg.message = "create or get engine function is not implemented for this data storage engine"
    raise self._msg
    
  def loadData(self):
    self._msg.code = -1
    self._msg.message = "Load data function is not implemented for this data storage engine"
    raise self._msg
    
  def saveData(self, path):
    self._msg.code = -1
    self._msg.message = "Save data function is not implemented for this data storage engine"
    raise self._msg