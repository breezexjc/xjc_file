from ..dao.TestModelTestMappper import *
class TestModelTestSvcImpl():
    def addOneRecode(self, vo):
        mapper=TestModelTestMappper()
        mapper.addOneRecord(vo)

