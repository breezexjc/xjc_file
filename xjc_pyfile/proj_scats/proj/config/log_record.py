import logging
import os

#
# path = os.path.abspath(__file__) + '\\log_record'
# print(path)
foldername = 'log_record'
filename = 'test'

class LogRecord():
    instance = None
    def __init__(self, foldername=foldername, filename=filename, path=None):
        """
        初始化日志文件
        :param foldername: 文件夹名
        :param filename: 文件名
        :param path: 路径, 默认当前文件夹
        """
        if path:
            self.path = path
        else:
            self.path = os.path.abspath('.')
        self.folder = foldername
        self.file = filename

    @classmethod
    def get_instance(cls, ):
        if cls.instance:
            return cls.instance
        else:
            obj = cls()
            cls.instance = obj
            return obj


    def create_log(self, filename,console=False):
        self.file = filename
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        if not os.path.exists(self.path + "\\" + self.folder):  # 文件夹不存在
            os.makedirs(self.path + "\\" + self.folder)
        handler = logging.FileHandler(self.path + "\\" + self.folder + "\\" + self.file + ".txt")
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(filename)s - [line:%(lineno)d] - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if console is True:
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            logger.addHandler(console)
        logger.addHandler(handler)
        return logger


if __name__ == "__main__":
    # log = LogRecord('log', 'test')
    # log = LogRecord('log', 'test', "E:\PycharmProjects")
    # logger = log.create_log()
    # try:
    #     if 1:
    #         logger.info(11)
    #
    # except Exception as e:
    #     logger.error(e)
    L = LogRecord()
    L.create_log('123')

