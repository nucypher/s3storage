"""Prototype S3 Storage

Design:

- Data stored in bucket prefix

- Assume that only one client touching a bucket/prefix

- Data organization:

  Data records: data/ioid/itid {ioid -> {itid -> data}}

  Temporary transaction records: commit/ittid/oid
                                 commit/ittid/data

  Transaction records: transactions/tid contains just meta data

  Last transaction id: ltid

  Where "i" prefix indicates that the value is "inverted" so that the
  maximum value comes first.  We do this to enable findin the maximum
  value by listing the keys for a prefix and picking the first.

- Committing:

  - Don't lock until tpc_vote (do conflict detection then).

  - On tpc_finish, write tid to commit/ttid/tid, then
    rename oid records, remove commit/ttid/tid.

    If we fail, then we try again on restart.


"""

import boto3
import botocore.exceptions
import datetime
import functools
import logging
import pickle
import threading
import ZODB.POSException

from six.moves.queue import Queue

from ZODB.utils import p64, u64, z64, newTid

logger = logging.getLogger(__name__)

m64 = (1 << 64) - 1
def invert64(i):
    """Invert an id (oid or tid) so that large values sort low

    And encode as hex for use in s3.

    This helps find the largest id.
    """
    return hex(m64 - u64(i))[2:]

def uninvert64(i):
    """Decode from hex and uninvert an inverted id (oid or tid)

    This helps find the largest id.
    """
    return p64(m64 - int(i, 16))

class Future:

    def __init__(self, func):
        self.func = func
        self.event = threading.Event()

    def set(self, value=None, exc=None):
        self.value = value
        self.exc = exc
        self.event.set()

    def get(self):
        self.event.wait()
        if self.exc:
            raise self.exc
        return self.value

    def run(self, s3):
        try:
            self.set(self.func(s3))
        except Exception as exc:
            self.set(exc=exc)
            logger.exception(self.func.__name__)

def s3method(func, async=False):

    @functools.wraps(func)
    def method(self, *args, **kw):

        @Future
        def future(s3):
            return func(self, s3.Bucket(self._bucket_name), *args, **kw)

        self.queue.put(future)

        if async:
            return future
        else:
            return future.get()

    return method

def s3asyncmethod(func):
    return s3method(func, True)

def worker(bucket, prefix, queue):
    get = queue.get
    session = boto3.session.Session()
    s3 = session.resource('s3')
    while 1:
        future = get()
        if future == 'stop':
            break
        future.run(s3)

class S3Storage(object):

    def __init__(self, bucket_name, prefix, pool_size=9):
        self._bucket_name = bucket_name
        if not prefix.endswith('/'):
            prefix += '/'
        self._prefix = prefix
        self._data_prefix = prefix + 'data/'
        self._commit_prefix = prefix + 'commit/'
        self._commiting = prefix + 'commiting'
        self._transaction_prefix = prefix + 'transactions/'
        self.__name__ = '{}:{}'.format(bucket_name, prefix)

        self.queue = queue = Queue()

        self.threads = [
            threading.Thread(
                target=worker, args=(bucket_name, prefix, queue),
                daemon=True,
                name='S3Storage{}:{}worker{}'.format(
                    self.__name__, id(self), i),
                )
            for i in range(pool_size)
            ]

        for thread in self.threads:
            thread.start()

        ltid = self._async(_get, prefix + 'ltid')
        oid = self._async(_first, self._data_prefix, -2)

        committing = self._get_committing()
        if committing[0]:
            # We dies without finishing a commit. Finish it now.
            self._finish_committing(*committing)
        else:
            # Clean up any left over uncommitted data
            self._async(_delete_keys, self._keys(self._commit_prefix))

        self._ltid = ltid = ltid.get()
        self._oid = u64(oid.get() or z64)

        self._lock = threading.RLock()
        self._commit_lock = threading.Lock()
        self._finish_lock = threading.Lock()

    @s3asyncmethod
    def _async(self, bucket, func, *args, **kw):
        return func(bucket, *args, **kw)

    def close(self):
        for thread in self.threads:
            self.queue.put('stop')
        for thread in self.threads:
            thread.join(9)

    def getName(self):
        return self.__name__

    @s3method
    def getSize(self, bucket):
        return sum(o.size for o in _it(bucket, self._prefix))

    def getTid(self, oid):
        with self._finish_lock:
            tid = self._getTid(oid).get()
        if tid is None:
            raise ZODB.POSException.POSKeyError(oid)
        else:
            return tid

    @s3asyncmethod
    def _getTid(self, bucket, oid):
        return _first(bucket, self._data_prefix + invert64(oid) + '/', -1)

    def isReadOnly(self):
        return False

    def lastTransaction(self):
        return self._ltid or z64

    def __len__(self):
        # Weird, tests expect this to be correct, or always 0. :/
        return 0

    def load(self, oid, version=''):
        with self._finish_lock:
            return self._load(oid).get()

    def loadBulk(self, oids):
        with self._finish_lock:
            return _fmap(self._load, oids)

    @s3asyncmethod
    def _load(self, bucket, oid):
        obr = _first(bucket, self._data_prefix + invert64(oid) + '/')
        if obr is None:
            raise ZODB.POSException.POSKeyError(oid)

        tid = uninvert64(obr.key.rsplit('/', 1)[1])
        data = obr.get()['Body'].read()
        return data, tid

    @s3method
    def loadBefore(self, bucket, oid, tid):
        prefix = self._data_prefix + invert64(oid) + '/'
        itid = invert64(tid)
        ltid = obr = None
        for obr in _it(bucket, prefix):
            otid = obr.key.rsplit('/', 1)[1]
            if otid > itid:
                break
            ltid = otid
        else:
            if ltid is None:
                raise ZODB.POSException.POSKeyError(oid)
            return None

        tid = uninvert64(otid)
        data = obr.get()['Body'].read()
        end = ltid
        if end:
            end = uninvert64(end)
        return data, tid, end

    @s3method
    def loadSerial(self, bucket, oid, serial):
        key = self._data_prefix + invert64(oid) + '/' + invert64(serial)
        data = _get(bucket, key)
        if data is None:
            raise ZODB.POSException.POSKeyError(oid, serial)

        return data

    def new_oid(self):
        with self._lock:
            self._oid += 1
            return ZODB.utils.p64(self._oid)

    def registerDB(self, db):
        pass

    def sortKey(self):
        return self.__name__

    def tpc_begin(self, transaction, tid=None):
        try:
            transaction.__data
        except AttributeError:
            pass
        else:
            raise ZODB.POSException.StorageTransactionError(
                "Multiple calls to tpc_begin in transactiuon", transaction)

        prefix = '{}{}_{}/'.format(
            self._commit_prefix,
            datetime.datetime.utcnow().isoformat(),
            id(transaction),
            )

        transaction.__data = tdata = TransactionData(
            tid=tid, prefix=prefix,
            user=transaction.user,
            description=transaction.description,
            extension=transaction._extension,
            )

        tdata.save = self._async(
            _put, prefix + 'data',
            pickle.dumps(dict(
                user=tdata.user,
                description=tdata.description,
                extension=tdata.extension,
                )),
            )

    def store(self, oid, serial, data, version, transaction):
        try:
            tdata = transaction.__data
        except AttributeError:
            raise ZODB.POSException.StorageTransactionError(self, transaction)
        if oid in tdata.stores:
            raise AssertionError("Duplicate oid", oid)
        tdata.checks[oid] = serial
        tdata.stores[oid] = self._async(
            _put, tdata.prefix + invert64(oid), data)

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        try:
            tdata = transaction.__data
        except AttributeError:
            raise ZODB.POSException.StorageTransactionError(self, transaction)

        tdata.checks[oid] = serial

    def tpc_vote(self, transaction):
        self._commit_lock.acquire()
        try:
            try:
                tdata = transaction.__data
            except AttributeError:
                raise ZODB.POSException.StorageTransactionError(
                    "tpc_finish called with wrong transaction")

            committed = { oid: self._getTid(oid) for oid in tdata.checks}

            for oid, serial in tdata.checks.items():
                old_tid = committed[oid].get()
                if old_tid and old_tid != serial:
                    # XXX conflict resolution
                    if oid in tdata.stores:
                        class_ = ZODB.POSException.ConflictError
                    else:
                        class_ = ZODB.POSException.ReadConflictError
                    raise class_(oid=oid, serials=(old_tid, serial))

            if tdata.tid is None:
                tdata.tid = ZODB.utils.newTid(self._ltid)

            # wait for saves:
            tdata.save.get()
            for store in tdata.stores.values():
                store.get()

            self._transaction = transaction

            tid = tdata.tid
            return [(oid, tid) for oid in tdata.stores]
        except Exception:
            self._transaction = None
            self._commit_lock.release()
            raise

    def tpc_finish(self, transaction, func = lambda tid: None):
        if (transaction is not self._transaction):
            raise ZODB.POSException.StorageTransactionError(
                "tpc_finish called with wrong transaction")

        try:
            tdata = transaction.__data
            prefix = tdata.prefix
            tid = tdata.tid

            self._async(
                _put, self._prefix+'committing', pickle.dumps((prefix, tid))
                ).get()
            self._ltid = tid
            with self._finish_lock:
                func(tid)
                self._finish_committing(prefix, tid)

            self._transaction = None
            del transaction.__data
        finally:
            self._commit_lock.release()

    def _finish_committing(self, prefix, tid):
        ltid = self._async(_put, self._prefix + 'ltid', tid)

        itid = invert64(tid)
        def mv(key):
            ioid = key.rsplit('/', 1)[1]
            if ioid == 'data':
                tkey = self._transaction_prefix + hex(u64(tid))[2:]
            else:
                tkey = '{}{}/{}'.format(self._data_prefix, ioid, itid)
            return self._mv(key, tkey)

        _fmap(mv, self._keys(prefix))

        ltid.get()

        self._async(_delete, self._prefix+'committing').get()

    @s3asyncmethod
    def _mv(self, bucket, fkey, tkey):
        bucket.Object(tkey).copy_from(
            CopySource=dict(Bucket=self._bucket_name, Key=fkey))
        bucket.Object(fkey).delete()

    def tpc_abort(self, transaction):
        try:
            tdata = transaction.__data
        except AttributeError:
            return

        if transaction == self._transaction:
            self._transaction = None
            self._commit_lock.release()

        if self._get_committing()[0] == tdata.prefix:
            raise ZODB.POSException.StorageTransactionError(
                "tpc_abort called with finished transaction")

        self._async(_delete_keys, tdata.prefix)

        del transaction.__data

    _transaction = None
    def tpc_transaction(self):
        return self._transaction

    @s3method
    def _get_committing(self, bucket):
        p = _get(bucket, self._prefix+'committing')
        if p is None:
            return None, None
        else:
            return pickle.loads(p)

    @s3method
    def _keys(self, bucket, prefix):
        return [o.key for o in _it(bucket, prefix)]


def _first(bucket, prefix, index=None, uninvert=True):
    try:
        r = next(iter(bucket.objects.filter(Prefix=prefix, MaxKeys=1)))
    except StopIteration:
        return None

    if index is not None:
        r = r.key.split('/')[index]
        if uninvert:
            r = uninvert64(r)
    return r

def _it(bucket, prefix):
    return bucket.objects.filter(Prefix=prefix)

def _fmap(func, it):
    futures = [func(i) for i in it]
    return [f.get() for f in futures]

def _get(bucket, key):
    try:
        data = bucket.Object(key).get()
    except botocore.exceptions.ClientError as e:
        if 'NoSuchKey' in str(e):
            return None
        else:
            raise
    else:
        return data['Body'].read()

def _put(bucket, key, value):
    bucket.put_object(Key=key, Body=value)

def _delete(bucket, key):
    bucket.Object(key).delete()

def _delete_keys(bucket, keys):
    if isinstance(keys, str):
        keys = [o.key for o in _it(bucket, keys)]
    while keys:
        bucket.delete_objects(
            Delete=dict(Objects=[dict(Key=key) for key in keys[:999]]))
        del keys[:999]

class TransactionData:

    status = ' '

    def __init__(self, **kw):
        self.__dict__.update(kw)
        self.checks = {}
        self.oids = {}
        self.stores = {}
