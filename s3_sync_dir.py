#!/proj/systems/apps/anaconda/bin/python
#
# Script to sync an entire directory to a s3 bucket and optionally delete local files
#
# Author: Kamil Czauz

import boto3, botocore
from botocore.client import Config
import sys, json, os, time, threading
import logging, argparse
import multiprocessing, copy_reg, types
import traceback

def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

copy_reg.pickle(types.MethodType, _pickle_method)

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


class Sync(object):
    def __init__(self, dirs_to_sync, bucket, boto_config_file, num_procs,
                 delete_local_files = False, verbosity = 0, aws_kms_key_id = None, 
                 s3_base_key = None, older_than_days = 0):

        # verbosity level
        self.set_verbosity( verbosity)

        # number of simultaneous processes
        self.num_procs = num_procs

        # delete local files after uploaded to s3
        self.delete_local_files = delete_local_files

        # encryption and authentication details
        self.aws_encryption_key_id = aws_kms_key_id

        # only care about files older than X days
        time_in_secs = older_than_days * 24 * 60 * 60
        self.min_mod_time = time.time() -  time_in_secs

        # set credentials env variable
        if ( os.path.isfile(boto_config_file) ):
            os.environ["BOTO_CONFIG"] = boto_config_file
        else:
            log.critical("This is not a boto config file {0}".format(boto_config_file))
            sys.exit(1)


        # s3 bucket / key details
        self.bucket = bucket
        self.s3_key_prefix = s3_base_key

        # verify dirs to sync exist
        for base_dir in dirs_to_sync:
            if ( not os.path.exists(base_dir) ):
                log.critical("{0} is not a valid directory to sync".format(dir_to_sync))
                sys.exit(1)

        self.dirs_to_sync = dirs_to_sync


        # files that have been uploaded to S3 and should be deleted
        self.uploaded_to_s3 = []
        self.failed_files = []

    # scan path for files that meet certain age criteria
    def gen_file_paths(self, base_dir):
        for (dirpath, dirnames, filenames) in os.walk(base_dir):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)


                # construct s3 key, relative to the base path
                base_key = os.path.relpath(full_path, base_dir)

                # add s3_key_prefix to the base key
                s3_key = os.path.join(self.s3_key_prefix, base_key)

                # only generate the filepaths that meet the age criteria
                try:
                    stat = os.stat(full_path)
                    if stat.st_mtime <= self.min_mod_time:
                        yield (full_path,s3_key,stat.st_mtime)
                except:
                    log.error( "Failed to get mtime for {0}".format(full_path) )
                    self.failed_files.append(full_path)
                    continue

    # return true if file size on s3 matches the local file size
    def verify_file_sizes(self, s3_key, full_path):
        try: 
            # test to see if key already exists first
            s3_file = s3_client.Object(bucket_name = self.bucket, key = s3_key)
            s3_file_size = s3_file.content_length
            local_file_size = os.path.getsize(full_path)
            if int(s3_file_size) == int(local_file_size):
                return True
            else:
                return False
        except:
            log.error("Error verifying sizes path={0} , s3_key={1}".format(full_path,s3_key))
            traceback.print_exc()
            return False

    def upload_file_to_s3(self, file_details):

        # decouple the tuple
        file_path, s3_key, mtime = file_details

        if self.s3_key_exists(s3_key):
            log.info("s3_key={0} already exists in the bucket {1}'".format(s3_key,self.bucket))
            self.uploaded_to_s3.append(file_path)
            self.remove_local_file(file_path, s3_key)

        else:
            log.debug("fullpath = {0} , s3_key = {1}".format(file_path, s3_key))
            
            if self.aws_encryption_key_id is None:
                extra_args = { 'Metadata': {"orig_mtime": str(mtime) } }
            else:
                extra_args = {
                              'SSEKMSKeyId': self.aws_encryption_key_id,
                              'ServerSideEncryption': 'aws:kms',
                              'Metadata': {"orig_mtime": str(mtime) }
                             }

            if self.verbosity >= 2:
                callback_process = ProgressPercentage(file_path)
            else:
                callback_process = None


            try:
                # perform the actual upload
                s3_client.meta.client.upload_file(
                                Filename = file_path, 
                                Bucket = self.bucket, 
                                Key = s3_key,
                                Config = s3_transfer_config,
                                Callback = callback_process,
                                ExtraArgs = extra_args
                )

                self.uploaded_to_s3.append(file_path)
                log.info("Upload successful, path={0} , s3_key={1}".format(file_path, s3_key))
                self.remove_local_file(file_path, s3_key)
            except:
                log.error("Upload failed, path={0}, s3_key={1}".format(file_path,s3_key) )
                traceback.print_exc()
                self.failed_files.append(file_path)
                return



    # return true if key in s3 exists
    def s3_key_exists(self, s3_key):
        try: 
            # test to see if key already exists first
            s3_client.Object(bucket_name = self.bucket, key = s3_key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # doesn't exist
                return False
            else:
                # some other problem...
                raise
        else:
            # key exists
            return True


    # delete local files if told to do so
    def remove_local_file(self, file_path, s3_key):
        if self.delete_local_files:
            # lets double check the files match in size
            if self.verify_file_sizes(s3_key, file_path):
                try:
                    os.unlink(file_path)
                    log.info("Deleted local file path={0}".format(file_path))
                except OSError as e:
                    traceback.print_exc()
                    log.error("Failed to delete local file path={0}".format(file_path))
            else:
                log.error("File sizes don't match, not deleting path={0} , s3_key={1}".format(file_path, s3_key))


    def set_verbosity(self, level):
        if level >=2 :
            log.setLevel(logging.DEBUG)
            self.verbosity = 2
        elif level >=1:
            log.setLevel(logging.INFO)
            self.verbosity = 1
        else:
            log.setLevel(logging.WARNING)
            self.verbosity = 0

### Main
if __name__ == "__main__":
    # configure logger
    logging.basicConfig(format="%(asctime)s [%(levelname)-5.5s]  %(message)s", level=logging.WARNING)
    log = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Sync directory to Amazon S3",
                                     usage="s3_sync_dir <source_directory> [<args>]" )
    # required arguments
    parser.add_argument('source_directory', type=str, nargs='+',
                        help="Directory(ies) to sync to s3")
    required_named = parser.add_argument_group('required arguments')
    required_named.add_argument("--bucket", default=None, required=True,
                        help="Specify which bucket to place objects into")
    required_named.add_argument("--boto-config-file", default=None, required=True,
                        help="Specify a boto config file that contains your aws credentials")

    # optional arguments
    parser.add_argument("-v", "--verbosity", action="count", default=0,
                        help="increase output verbosity")
    parser.add_argument("--older-than-days", type=int, default=0,
                        help="only upload files older than a specified number of days")
    parser.add_argument("--delete-local-files", action='store_true',
                    help="Delete the local file if the file is successfully uploaded to s3 or it already exists in s3")
    parser.add_argument("--base-key", default=None,
                        help="Specify a base key to prepend to the s3 object key")
    parser.add_argument("--aws-kms-encryption-key", default=None,
                        help="Specify an aws kms encryption key id to use to encrypt data uploaded to s3")
    parser.add_argument("--num-procs", default=5, type=int,
                        help="number of files to upload simultaneously (number of processes to spawn) ")

    # parse arguments
    args = parser.parse_args()

    # instantiate class object
    s3sync = Sync(args.source_directory,
                  args.bucket,
                  args.boto_config_file,
                  args.num_procs,
                  args.delete_local_files,
                  verbosity = args.verbosity,
                  aws_kms_key_id = args.aws_kms_encryption_key,
                  s3_base_key = args.base_key,
                  older_than_days = args.older_than_days)

    # S3 client object
    # apparently this cant be inside a class object, since this client is not pickleable
    s3_client = boto3.resource('s3', config=Config(signature_version='s3v4'))

    # transfer config for large multipart file uploads
    # current limit is for a single file of 625G  = (64 chunk_size_MB * 10,000 max_num_s3_parts) / 1024 MB_to_G
    s3_transfer_config = boto3.s3.transfer.TransferConfig(
                         multipart_threshold=128 * 1024 * 1024,
                         max_concurrency=8,
                         num_download_attempts=10,
                         multipart_chunksize=64 * 1024 * 1024,
                         max_io_queue=10000 )

    for sync_dir in s3sync.dirs_to_sync:
        log.info("Starting to sync {0}".format(sync_dir))
        pool = multiprocessing.Pool(s3sync.num_procs)
        pool.map(s3sync.upload_file_to_s3, s3sync.gen_file_paths(sync_dir))
        log.info("Finished syncing {0}".format(sync_dir))
