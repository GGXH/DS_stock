import util.dataframeIF.dataframeIF as dataframeIF
import util.status.messager as msg

class dataframeSP(dataframeIF):
  def __init__(self, name, engine = None, data = None):
    dataframeIF.__init__(name, data)
    self._engine = engine
    
  @property
  def col_name(self):
    self._msg.code = -1
    self._msg.message = "Column names property is not implemented for this data frame engine"
    raise self._msg
  
  @property
  def data(self):
    return self._data
    
  @data.setter
  def data(self, data):
    self._data = data
    
  @property
  def engine(self):
    return self._engine
    
  @engine.setter
  def engine(self, engine):
    self._engine = engine
    
  @property
  def name(self):
    return self._name
   
  @name.setter
  def name(self, name):
    self._name = name

  def check_vars_in_col(self, var_list):
    self._msg.code = -1
    self._msg.message = "Check vars in column function is not implemented for this data frame engine"
    raise self._msg
    
  def _check_var_in_col(self, var):
    self._msg.code = -1
    self._msg.message = "Check var in column function is not implemented for this data frame engine"
    raise self._msg
    
  def colum_equal(self, df):
    self._msg.code = -1
    self._msg.message = "Colum equal function is not implemented for this data frame engine"
    raise self._msg
    
  def load_data(self):
    self._msg.code = -1
    self._msg.message = "Load data function is not implemented for this data frame engine"
    raise self._msg
    
  def save_data(self):
    self._msg.code = -1
    self._msg.message = "Save data function is not implemented for this data frame engine"
    raise self._msg