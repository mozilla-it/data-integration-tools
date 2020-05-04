
import pysftp
import csv
import os

class SFTP:
    @staticmethod
    def write_to_spaceiq(local_path,remote_path,**creds):
        opts = pysftp.CnOpts()
        opts.hostkeys = None
        host = creds.get("host",None)
        username = creds.get("username",None)
        private_key_path = creds.get("private_key_path")
        sftp = pysftp.Connection(host=host,username=username,private_key=private_key_path,cnopts=opts)
        print(sftp.put(local_path, remote_path))

    @staticmethod
    def format_csv(filename):
        with open(filename) as csvreader:
            reader = csv.reader(csvreader)
            rows = list(reader)
        new_filename = f"{filename}.new"
        with open(new_filename,'w') as csvwriter:
            writer = csv.writer(csvwriter, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerows(rows)
        os.remove(filename)
        os.rename(new_filename,filename)


