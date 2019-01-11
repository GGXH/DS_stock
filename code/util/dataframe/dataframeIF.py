import util.status.messager as msg

class dataframe:
  def __init__(self, name, data = None):
    self._name = name
    self._data = data
    self._msg = msg()
    
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