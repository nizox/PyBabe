from base import BabeBase
from subprocess import Popen, PIPE
import os.path
from gzip import GzipFile
import tempfile 

def compress(compress_outstream, inputfile_filename, inarchive_filename):
	f = open(compress_outstream, 'w')
	p = Popen(['gzip', '-c', inputfile_filename], stdout=f)
	p.communicate()
	f.close()

def get_content_list(compress_instream, filename):
	if not hasattr(compress_instream, 'fileno'): 
		tf = tempfile.NamedTemporaryFile()
		tf.write(compress_instream.read())
		tf.flush()
		p = Popen(['gzip', '-d', '-c', tf.name], stdin=None, stdout=PIPE)
	else: 
		tf = None
		p = Popen(['gzip', '-d', '-c'], stdin=compress_instream,  stdout=PIPE)
	f = os.path.splitext(os.path.basename(filename))[0] if filename else None
	return (( tf, p.stdout ) , [f])
     
def uncompress(handle, name):
	return handle[1]
    
BabeBase.addCompressPushPlugin('gz', ['gz'], compress)
BabeBase.addCompressPullPlugin('gz', ['gz'], get_content_list, uncompress, need_seek=False)