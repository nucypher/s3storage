********************
Prototype S3 Storage
********************

Usage::

  import s3storage

  storage = s3storage.S3Storage(bucket_name, bucket_prefix, pool_size)

Where:

  bucket_name
     S3 bucket name

  bucket_prefix
     Prefix to add to keys created by the storage.

     This can be an empty string, but using a non-empty string allows
     multiple keys in the same bucket.

To run the tests, you need to set the S3STORAGE_TEST_BUCKET to a
bucket name, optionally followed by ':' and a bucket prefix.

To-do (Maybe):

- Conflict resolution

- Packing

- Iteration

- History

- ZConfig support

Changes
*******

0.1.0 (yyyy-mm-dd)
==================

Initial release
