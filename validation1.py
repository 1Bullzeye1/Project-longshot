# from org.apache.hadoop.hive.ql.exec import UDF
# from org.apache.hadoop.io import Text
# from org.apache.commons.lang import StringUutils
#
# class RemoveChar(org.apache.hadoop.hive.ql.exec.UDF):
#
#     def __init__(self):
#
#         self.colValue = org.apache.hadoop.io.Text()
#
#     def evaluate(self, str, charRemove):
#         if str is None:
#             return str
#         self.colValue.set(StringUtils.strip(str(str), charRemove))
#         return self.colValue
