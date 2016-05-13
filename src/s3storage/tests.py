import os
import boto3
import random
import unittest

from . import S3Storage, m64, _delete_keys

from ZODB.tests import (
    BasicStorage,
    HistoryStorage,
    IteratorStorage,
    MTStorage,
    PackableStorage,
    RevisionStorage,
    StorageTestBase,
    Synchronization,
    )

class Tests(
    StorageTestBase.StorageTestBase,
    BasicStorage.BasicStorage,
    MTStorage.MTStorage,
    RevisionStorage.RevisionStorage,
    Synchronization.SynchronizedStorage,

    # Not currently relevant:

    # PackableStorage.PackableStorageWithOptionalGC,
    # IteratorStorage.ExtendedIteratorStorage,
    # IteratorStorage.IteratorStorage,
    # HistoryStorage.HistoryStorage,
    ):

    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self, )

        self.__bucket_name = os.environ['S3STORAGE_TEST_BUCKET']
        if ':' in self.__bucket_name:
            (self.__bucket_name, self.__base_prefix
             ) = self.__bucket_name.split(':')
        else:
            self.__base_prefix = 'TEST'

        if self.__base_prefix and not self.__base_prefix.endswith('/'):
            self.__base_prefix += '/'

        self.__prefix = '{}{}'.format(
            self.__base_prefix, random.randint(0, m64))
        self._storage = S3Storage(self.__bucket_name, self.__prefix)

    def tearDown(self):
        self._storage.close()
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.__bucket_name)
        _delete_keys(bucket, self.__prefix)

    def checkOversizeNote(self):
        # This base class test checks for the common case where a storage
        # doesnt support huge transaction metadata. This storage doesnt
        # have this limit, so we inhibit this test here.
        pass

    def checkLoadBeforeUndo(self):
        pass # we don't support undo yet
    checkUndoZombie = checkLoadBeforeUndo

def test_suite():
    return unittest.makeSuite(Tests, 'check')

if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
