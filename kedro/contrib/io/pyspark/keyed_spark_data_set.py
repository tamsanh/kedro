import abc
import threading
from pathlib import PosixPath
from typing import Dict, List

from pyspark.sql.utils import AnalysisException

from kedro.contrib.io.pyspark import SparkDataSet
from pyspark.sql import DataFrame
from kedro.contrib.io.pyspark.spark_data_set import _strip_dbfs_prefix
from kedro.io import DataSetError


class KeyedSparkData(dict):
    def __init__(self, key):
        super().__init__()
        self.key = key

    def get_latest_data_key(self):
        return max(self.keys())

    def get_latest_data(self):
        return self[self.get_latest_data_key()]


class KeyedSparkDataSet(SparkDataSet, abc.ABC):

    def __init__(self, *args, key=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    @abc.abstractmethod
    def get_data_keys(self):
        pass

    def _generate_path_from_value(self, base_path, value):
        key_value = '%s=%s' % (self.key, value)
        return str(PosixPath(base_path, key_value))

    def _load(self) -> KeyedSparkData:
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        loaded_dict = KeyedSparkData(self.key)
        data_keys = self.get_data_keys()
        threads = []

        def _load_data_key():
            while True:
                try:
                    data_key = data_keys.pop()
                except IndexError:
                    return
                target_load_path = self._generate_path_from_value(load_path, data_key)
                loaded_dict[data_key] = self._get_spark().read.load(
                    target_load_path, self._file_format, **self._load_args
                )

        for ti in range(5):
            t = threading.Thread(target=_load_data_key, name='KeyedSparkDataLoader %s' % ti)
            t.daemon = True
            threads.append(t)
            t.start()

        for t in threads:
            t.join(999999999)

        return loaded_dict

    def _save(self, data: Dict[str, DataFrame]) -> None:
        save_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))
        data_items = data.items()

        def _save_data_item():
            while True:
                try:
                    data_key, df = data_items.pop()
                except IndexError:
                    return
                target_save_path = self._generate_path_from_value(save_path, data_key)
                df.write.save(target_save_path, self._file_format, **self._save_args)

        threads = []
        for ti in range(5):
            t = threading.Thread(target=_save_data_item, name='KeyedSparkDataLoader %s' % ti)
            t.daemon = True
            threads.append(t)
            t.start()

        for t in threads:
            t.join(999999999)

    def _exists(self) -> bool:
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        try:
            self._get_spark().read.load(load_path, self._file_format)
        except AnalysisException as exception:
            if exception.desc.startswith("Path does not exist:"):
                return False
            raise
        return True


class FailedToRetrieveAlluxioList(DataSetError):
    pass


class AlluxioKeyedSparkDataSet(KeyedSparkDataSet):

    @staticmethod
    def _parse_alluxio_output(alluxio_output) -> List[str]:
        """Determines whether given ``hdfs_path`` exists in HDFS.

        Args:
            alluxio_output: The raw alluxio command data that is output by an alluxio command

        Returns:
            A list of alluxio paths
        """
        paths = []
        for line in alluxio_output.strip().split('\n'):
            paths.append(line.strip().split(' ')[-1])

        return ['alluxio://%s' % p for p in paths]

    @staticmethod
    def _alluxio_ls(path, retry_num=0):
        max_retries = 5
        import subprocess
        pobj = subprocess.Popen(
            ['alluxio', 'fs', 'ls', path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = pobj.communicate()
        if pobj.returncode and retry_num > max_retries:
            raise FailedToRetrieveAlluxioList(err.decode('utf8'))
        return out.decode('utf8')

    def get_data_keys(self) -> List[str]:
        """Gets the data keys for the file path

        Returns:
            A list of key_values for the file path
        """
        alluxio_output = self._alluxio_ls(self._filepath)
        alluxio_paths = self._parse_alluxio_output(alluxio_output)

        data_keys = []
        for path in alluxio_paths:
            _, data_key = path.split('%s=' % self.key)
            data_keys.append(data_key)
        return data_keys
