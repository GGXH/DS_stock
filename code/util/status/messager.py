class messager:
 def __init__(self):
  self._code = 0
  self._message = ""
  self._sys_message = ""
  
 def __str__(self):
  msg_str = ""
  msg = self.get_msg()
  for k in sorted(msg.keys()):
   msg_str += k
   msg_str += ": "
   msg_str += msg[k]
   msg_str += ";"
  return msg_str
  
 @property
 def code(self):
  return self._code
  
 @code.setter
 def code(self, code):
  self._code = 
  
 @property
 def message(self):
  return self._message
  
 @message.setter
 def message(self, msg):
  self._message = msg
  
 @property
 def sys_message(self):
  return self._sys_message
  
 @sys_message.setter
 def sys_message(self, sys_msg):
  self._sys_message = sys_msg
  
 def clear(self):
  self._code = 0
  self._message = ""
  self._sys_message = ""
  
 def get_msg(self):
  return {"code" : self._code, 
          "message": self._message,
          "sys_message": self._sys_message}
 
 def set_msg(self, msg):
  self.clear()
  if "code" in msg:
   self.code = msg["code"]
  
  if "message" in msg:
   self.message = msg["message"]
   
  if "sys_message" in msg:
   self.sys_message = msg["sys_message"]
   
 
