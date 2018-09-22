#!/usr/bin/env python

from __future__ import print_function, absolute_import, division
import logging
import xmlrpclib,pickle,hashlib,socket
from collections import defaultdict
from errno import ENOENT
from errno import ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time,sleep

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
						
if not hasattr(__builtins__, 'bytes'):
    bytes = str
 
class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports multilevel of files.'

    def __init__(self,metaserverport,dataserverport):
        #self.files = {}	
	self.size_block=8							
	self.data = defaultdict(list)						
        self.fd = 0								
	self.metaserverport=metaserverport
	self.dataserverport=dataserverport
	print(self.metaserverport)
	print(self.dataserverport)
	self.metaserveradd=xmlrpclib.ServerProxy('http://localhost:' + str(self.metaserverport) + '/')				
	self.dataserveradd=[]
	for i in range (0,len(self.dataserverport)):
		self.dataserveradd.append(xmlrpclib.ServerProxy('http://localhost:' + str(self.dataserverport[i]) + '/'))
	print (self.metaserveradd)
	print (self.dataserveradd)
        now = time()
	self.metaserveradd.put('/',pickle.dumps(dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,st_mtime=now, st_atime=now, st_nlink=2, files = [])))	
 	print(pickle.loads(self.metaserveradd.get('/')))

    def chmod(self, path, mode):
	metadata = pickle.loads(self.metaserveradd.get(path))
	metadata['st_mode'] &= 0o770000
        metadata['st_mode'] |= mode
	self.metaserveradd.put(path,pickle.dumps(metadata))
        return 0

    def chown(self, path, uid, gid):
	metadata = pickle.loads(self.metaserveradd.get(path))
        metadata['st_uid'] = uid
        metadata['st_gid'] = gid
	self.metaserveradd.put(path,pickle.dumps(metadata))
	

    def create(self, path, mode):
	metadata = dict(st_mode=(S_IFREG | mode), st_nlink=1,st_size=0, st_ctime=time(), st_mtime=time(),st_atime=time(),files=[],blocks=[])
	self.metaserveradd.put(path,pickle.dumps(metadata))
	parent, child=self.dividepath(path)				
	metadata = pickle.loads(self.metaserveradd.get(parent))
	metadata['files'].append(child)			
	self.metaserveradd.put(parent,pickle.dumps(metadata))
	self.fd += 1
        return self.fd

    def dividepath(self, path):
	child = path[path.rfind('/')+1:]											
	parent = path[:path.rfind('/')]
	if parent == '':
		parent='/'
	return parent,child

    def getattr(self, path, fh=None):
        if self.metaserveradd.get(path) == -1:
            raise FuseOSError(ENOENT)
	return pickle.loads(self.metaserveradd.get(path))

    def getxattr(self, path, name, position=0):
	if self.metaserveradd.get(path) == -1:
            return ''   							
	metadata = pickle.loads(self.metaserveradd.get(path))
	attrs = metadata.get('attrs', {})
	try:
            return attrs[name]
        except KeyError:
            return ''       		# Should return ENOATTR

    def listxattr(self, path):
	if self.metaserveradd.get(path) == -1:
            return ''   		# Should return ENOATTR
        metadata = pickle.loads(self.metaserveradd.get(path))
        attrs = metadata.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        metadata = dict(st_mode=(S_IFDIR | mode), st_nlink=2,st_size=0, st_ctime=time(), st_mtime=time(),st_atime=time(),files=[])
	self.metaserveradd.put(path,pickle.dumps(metadata))
	parent,child=self.dividepath(path)					
	metadata=pickle.loads(self.metaserveradd.get(parent))				
	metadata['st_nlink'] += 1	
	metadata['files'].append(child)
	self.metaserveradd.put(parent,pickle.dumps(metadata))

    def open(self, path, flags):
        self.fd += 1									# increment the self.fd
        return self.fd

											
    def read(self, path, size, offset, fh):
	metadata = pickle.loads(self.metaserveradd.get(path))
	data = self.readdata(path,metadata['blocks'])
        return data[offset:offset+size]

    def readdata(self,path,blocks):
        output = ''
	#storage1 = storage2 = storage3 = 0
        for i in range(0,len(blocks)):
            try:
                storage1 = self.dataserveradd[blocks[i]].get(path + str(i))
		
            except socket.error:
                print('WARNING READ - link to Data Server at port ' + str(self.dataserverport[blocks[i]]) + ' is lost.')
                storage1 = -1
            try:
                storage2 = self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].get(path + str(i))
		
            except socket.error:
                print('WARNING READ - link to Data Server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is lost.')
                storage2 = -1
	    try:
                storage3 = self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].get(path + str(i))
		print("localblock 3" + str(storage3))
            except socket.error:
                print('WARNING READ - link to Data Server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is lost.')
                storage3 = -1

            # All three copies are lost
            if (storage1 == -1) and (storage2 == -1) and (storage3 == -1):
                output = 'ERROR - Three adjacent servers lost their respective persistance storage blocks. Cannot RECOVER.'
                break
	    # All three copies are present
            elif (storage1 != -1) and (storage2 != -1) and (storage3 != -1):
                # verifying Checksum
                if (storage1[len(storage1) - 32:] == hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()) and (storage2[len(storage2) - 32:] == hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()) and (storage3[len(storage3) - 32:] == hashlib.md5(storage2[:len(storage3) - 32]).hexdigest()):
                    output += storage1[:len(storage1) - 32]

                elif (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()) and (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()) and (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
                    output = 'ERROR - All the copies are corrupted.'
                    for i in range(0,3):
                        sleep(2)
                    break
                elif (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()) and (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()):
                    print('First copy and Second copy is corrupted. Recovering....')
                    # Recovering First copy and Second copy
                    try:
			self.dataserveradd[blocks[i]].put(path + str(i),storage3)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
		    try:
			self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage3)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage3[:len(storage3) - 32]
		    print("output" + str(output))

		elif (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()) and (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
                    print('First copy and Third copy is corrupted. Recovering....')
                    # Recovering First copy and Third copy
                    try:
			self.dataserveradd[blocks[i]].put(path + str(i),storage2)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
		    try:
			self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage2)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage2[:len(storage2) - 32]
		    print("output" + str(output))

		elif (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()) and (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
                    print('Second copy and Third copy is corrupted. Recovering....')
                    # Recovering Second copy and Third copy
                    try:
			self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage1)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
		    try:
			self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage2)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage1[:len(storage1) - 32]
		    print("output" + str(output))
		
		elif (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()):
                    print('First copy is corrupted. Recovering....')
                    # Recovering First copy
                    try:
			self.dataserveradd[blocks[i]].put(path + str(i),storage3)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
                    output += storage3[:len(storage3) - 32]
		
		elif (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()):
                    print('Second copy is corrupted. Recovering....')
                    # Recovering Second copy
                    try:
			self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage1)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage1[:len(storage1) - 32]
		
		elif (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
                    print('Third copy is corrupted. Recovering....')
                    # Recovering Third copy
		    try:
			self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage2)
                    except socket.error:
                        print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage1[:len(storage1) - 32]
		    

                else:
                    output = 'ERROR - Unhandled Exception while verifyinging checksum'
                    break
	    # First copy is lost
	    elif (storage2 != -1) and (storage3 != -1):
                # verifying Checksum
                if (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()) and (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
                    output = 'ERROR - First copy is absent and Second and Third copy is corrupted.'
                    for i in range(0,3):
                        print(output)
                        sleep(2)
                    break
		# Recovering First and Second from Third copy
		elif (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()):
		    print('First copy is absent and Second copy is corrupted. Recovering....')
		    try:
                    	self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage3)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
		    try:
                    	self.dataserveradd[blocks[i]].put(path + str(i),storage3)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
		    output += storage3[:len(storage3) - 32]
		    print("output" + str(output))
		# Recovering First and Third from Second copy
		elif (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
		    print('First copy is absent and Third copy is corrupted. Recovering....')
		    try:
                    	self.dataserveradd[blocks[i]].put(path + str(i),storage2)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
		    try:
                    	self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage2)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage2[:len(storage2) - 32]
		    print("output" + str(output))
		else:
		    try:
                    	self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage2)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage2[:len(storage2) - 32]
		    print("output" + str(output))        
		
            # Second copy lost
            elif (storage1 != -1) and (storage3 != -1):
                # verifying Checksum
                if (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()) and (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
                    output = 'ERROR - Second copy is absent and First and Third copy is corrupted.'
                    for i in range(0,3):
                        print(output)
                        sleep(2)
                    break
		# Recovering First and Second from Third copy
		elif (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()):
		    print('Second copy is absent and First copy is corrupted. Recovering....')
		    try:
                    	self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage3)
                    except socket.error:
                    	print('WARNING- Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
		    try:
                    	self.dataserveradd[blocks[i]].put(path + str(i),storage3)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
		    output += storage3[:len(storage3) - 32]
		    print("output" + str(output))
		# Recovering Second and Third from First copy
		elif (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
		    print('Second copy is absent and Third copy is corrupted. Recovering....')
		    try:
                    	self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage1)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
		    try:
                    	self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage1)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage1[:len(storage1) - 32]
		    print("output" + str(output))
		else:
		    try:
                    	self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage1)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage1[:len(storage1) - 32]
		    print("output" + str(output))
	    # Third copy lost
	    elif (storage1 != -1) and (storage2 != -1):
                # verifying Checksum
                if (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()) and (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()):
                    output = 'ERROR - Third copy is absent and First and Second copy is corrupted.'
                    for i in range(0,3):
                        print(output)
                        sleep(2)
                    break
		# Recovering First and Third from Second copy
		elif (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()):
		    print('Third copy is absent and First copy is corrupted. Recovering....')
		    try:
                    	self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage2)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
		    try:
                    	self.dataserveradd[blocks[i]].put(path + str(i),storage2)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
		    output += storage2[:len(storage2) - 32]
		    print("output" + str(output))
		# Recovering Second and Third from First copy
		elif (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()):
		    print('Third copy is absent and Second copy is corrupted. Recovering....')
		    try:
                    	self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage1)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
		    try:
                    	self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage1)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage1[:len(storage1) - 32]
		    print("output" + str(output))
		else:
	    	    try:
                    	self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage1)
                    except socket.error:
                    	print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
                    output += storage1[:len(storage1) - 32]
		    print("output" + str(output))
	    # First and Second copies are lost 
	    elif (storage3 != -1):
	    # Third copy is corrupted
		if (storage3[len(storage3) - 32:] != hashlib.md5(storage3[:len(storage3) - 32]).hexdigest()):
		    output = 'ERROR - First and Second copy is absent and Third copy is corrupted.'
                    for i in range(0,3):
                        print(output)
                        sleep(3)
                    break
                # Recovering First and Second Copy
                try:
                    self.dataserveradd[blocks[i]].put(path + str(i),storage3)
                except socket.error:
                    print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
		try:
                    self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage3)
                except socket.error:
                    print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
		
                output += storage3[:len(storage3) - 32]
		print("output" + str(output))
	    # First and Third copies are lost
	    elif (storage2 != -1):
	    # Second copy is corrupted
		if (storage2[len(storage2) - 32:] != hashlib.md5(storage2[:len(storage2) - 32]).hexdigest()):
		    output = 'ERROR - First and Third copy is absent and Second copy is corrupted.'
                    for i in range(0,3):
                        print(output)
                        sleep(3)
                    break
                # Recovering First and Third Copy
                try:
                    self.dataserveradd[blocks[i]].put(path + str(i),storage2)
                except socket.error:
                    print('WARNING - Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent while data correction.')
		try:
                    self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage2)
                except socket.error:
                    print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
		
                output += storage2[:len(storage2) - 32]
		print("output" + str(output))

	     # Second and Third copies are lost
	    elif (storage1 != -1):
	    # First copy is corrupted
		if (storage1[len(storage1) - 32:] != hashlib.md5(storage1[:len(storage1) - 32]).hexdigest()):
		    output = 'ERROR - Second and Third copy is absent and First copy is corrupted.'
                    for i in range(0,3):
                        print(output)
                        sleep(3)
                    break
                # Recovering Second and Third Copy
                try:
		    self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),storage1)
                except socket.error:
                    print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent while data correction.')
		try:
                    self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),storage1)
                except socket.error:
                    print('WARNING - Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent while data correction.')
		
                output += storage1[:len(storage1) - 32]
		print("output" + str(output))
	    
	    else:
                output = 'ERROR - Unhandled Exception'
                break
        return output

        


    def readdir(self, path, fh):
	metadata=pickle.loads(self.metaserveradd.get(path))						# load metadata from the metaserver
	
	print (metadata)
        return ['.', '..'] + [x for x in metadata['files']]

    def readlink(self, path):
	p=pickle.loads(self.metaserveradd.get(path))
	data=self.readdata(path,p['blocks'])
	return data[offset:offset+size]
        return data

    def removexattr(self, path, name):
	if self.metaserveradd.get(path) == -1:
            return ''   # Should return ENOATTR
        metadata = pickle.loads(self.metaserveradd.get(path))
        attrs = metadata.get('attrs', {})

        try:
            del attrs[name]
            metadata.set('attrs', attrs)
            self.metaserveradd.put(path,pickle.dumps(metadata))
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
	metadataold=pickle.loads(self.metaserveradd.get(old))
	op,oc=self.dividepath(old)
	np,nc=self.dividepath(new)
	if metadataold['st_mode'] & 0770000 == S_IFDIR:						
		#self.mkdir(new,S_IFDIR)		
		self.metaserveradd.put(new,pickle.dumps(dict(st_mode=(S_IFDIR | 0o777), st_ctime=time(),st_mtime=time(), st_atime=time(), st_nlink=metadataold['st_nlink'], st_size=0, files = [])))
		metadata = pickle.loads(self.metaserveradd.get(np))
		metadata['files'].append(nc)
        	metadata['st_nlink'] += 1
        	self.metaserveradd.put(np,pickle.dumps(metadata))
		for files in metadataold['files']:				
			self.rename(old + '/' + files, new + '/' + files)
		self.rmdir(old)								
        else:
		#self.create(new,S_IFREG)							    		
		self.metaserveradd.put(new,pickle.dumps(dict(st_mode=(S_IFREG | 0o777), st_nlink=1, st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time(), files = [], blocks = [])))
        	metadata = pickle.loads(self.metaserveradd.get(np))
        	metadata['files'].append(nc)
        	self.metaserveradd.put(np,pickle.dumps(metadata))
        	self.fd += 1    
		metadatanew = metadataold
        	Data = self.readdata(old,metadataold['blocks'])
        	blockdata = []
        	blocks = []
        	pointer = hash(new)
        	count = 1
        	for i in range(0,len(Data),self.size_block):
            		blockdata.append(Data[i : i + self.size_block])
            		blocks.append((pointer + count - 1) % len(self.dataserverport))
            		count += 1;

        	self.writedata(new,blockdata,blocks)
        	metadatanew['st_size'] = len(Data)
        	metadatanew['blocks'] = blocks
		self.metaserveradd.put(new,pickle.dumps(metadatanew))		
		metadata_new = pickle.loads(self.metaserveradd.get(old))
        	if (metadata_new['st_mode'] & S_IFREG) == S_IFREG:
	    		blocks = metadata_new['blocks']
            	metadata_new = pickle.loads(self.metaserveradd.get(op))
            	metadata_new['files'].remove(oc)
            	self.metaserveradd.put(op,pickle.dumps(metadata_new))
		self.metaserveradd.pop_entry(old)
	
    def removedata(self,path,blocks):
        for i in range(0,len(blocks)):
            link = False
            while link is False:
                try:
                    link = self.dataserveradd[blocks[i]].pop_entry(path + str(i))
                except socket.error:
                    print('WARNING - Data Removal: Data server at port ' + str(self.dataserverport[blocks[i]]) + ' is absent.')
                    print('Trying to re-connect.....')
                    sleep(5)
            link = False
            while link is False:
                try:
                    link = self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].pop_entry(path + str(i))
                except socket.error:
                    print('WARNING - Data Removal: Data server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is absent.')
                    print('Trying to re-connect.....')
                    sleep(5)
            link = False
            while link is False:
                try:
                    link = self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].pop_entry(path + str(i))
                except socket.error:
                    print('WARNING - Data Removal: Data server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is absent.')
                    print('Trying to re-connect.....')
                    sleep(5)		

    def rmdir(self, path):
	parent,child=self.dividepath(path)
	meta_data_path=pickle.loads(self.metaserveradd.get(path))
	metadata=pickle.loads(self.metaserveradd.get(parent))	
	if meta_data_path['st_mode'] & 0770000 == S_IFDIR:
		if not meta_data_path['files'] == []:
			raise FuseOSError(ENOTEMPTY)						# raise a FUSE error.
	metadata['files'].remove(child)
	metadata['st_nlink'] -=1								# decrement the st_nlink
	self.metaserveradd.put(parent,pickle.dumps(metadata))
	self.metaserveradd.pop_entry(path)
        

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
	if self.metaserveradd.get(path) == -1:
            return ''   # Should return ENOATTR
        metadata = pickle.loads(self.metaserveradd.get(path))
        attrs = metadata.setdefault('attrs', {})
        attrs[name] = value
        metadata.set('attrs', attrs)
        self.metaserveradd.put(path,pickle.dumps(metadata))
             

    def statfs(self, path):
        return dict(f_bsize=self.size_block, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.metaserveradd.put(target,pickle.dumps(dict(st_mode=(S_IFLNK | 0o777), st_nlink=1, st_size=len(source), blocks = [], files=[])))
	metaData = pickle.loads(self.metaserveradd.get(target))
	x = hash(target)
        blocks_newdata = []
        blocks = []
        j = 1
        for i in range(0,len(source),self.size_block):
            blocks_newdata.append(source[i : i + self.size_block])
            blocks.append((x + j - 1) % len(self.dataserverport))
            j += 1;
        self.writedata(target,blocks_newdata,blocks)
	metaData['blocks'] = blocks
	self.metaserveradd.put(target,pickle.dumps(metaData))
	parentpath,childpath = self.splitpath(target)
	meta_data = pickle.loads(self.metaserveradd.get(parentpath))
        meta_data['files'].append(childpath)
        self.metaserveradd.put(parentpath,pickle.dumps(meta_data))


    def truncate(self, path, length, fh=None):
	h = hash(path)	
	blocks_newdata = []
        blocks = []
       	metadata = pickle.loads(self.metaserveradd.get(path))
        data1 = self.readdata(path,metadata['blocks'])
        data2 = data1[:length]
        n = 1
        for i in range(0,len(data2),self.size_block):
            blocks_newdata.append(data2[i : i + self.size_block])
            blocks.append((h + n - 1) % len(self.dataserverport))
            n += 1;
        self.writedata(path,blocks_newdata,blocks)
        #for i in range (0,len(secdata)):
	#	self.dataserveradd[blocks[i]].put(path+str(i),secdata[i])
	metadata['st_size'] = length
	metadata['blocks'] = blocks	
	self.metaserveradd.put(path,pickle.dumps(metadata))	

    def unlink(self, path):
	parent,child=self.dividepath(path)
	metadata=pickle.loads(self.metaserveradd.get(path))						
	self.metaserveradd.put(parent,pickle.dumps(metadata))	
        if (metadata['st_mode'] & S_IFREG) == S_IFREG:
	    self.removedata(path,metadata['blocks'])
	    parent,child=self.dividepath(path)
            meta_data = pickle.loads(self.metaserveradd.get(parent))
            meta_data['files'].remove(child)
            self.metaserveradd.put(parent,pickle.dumps(metadata))
	self.metaserveradd.pop_entry(path)
        

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
	metadata=pickle.loads(self.metaserveradd.get(path))
        metadata['st_atime'] = atime
        metadata['st_mtime'] = mtime
	self.metaserveradd.put(path,pickle.dumps(metadata))

	
    def write(self, path, data, offset, fh):	
	h = hash(path)	
	blocks_newdata = []
        blocks = []
        metadata = pickle.loads(self.metaserveradd.get(path))
        if len(metadata['blocks']) == 0:
            oldData = ''
        else:
            oldData = self.readdata(path,metadata['blocks'])
        newdata = oldData[:offset].ljust(offset,'\x00') + data + oldData[offset + len(data):]
        n = 1
        for a in range(0,len(newdata), self.size_block):
            blocks_newdata.append(newdata[a : a + self.size_block])
            blocks.append((h + n - 1) % len(self.dataserverport))
            n += 1;               	
        self.writedata(path,blocks_newdata,blocks)
	metadata['st_size'] = len(newdata)
        metadata['blocks'] = blocks
        self.metaserveradd.put(path,pickle.dumps(metadata))
        return len(data)				
	
    def writedata(self,path,blocks_newdata,blocks):
       	  for i in range(0,len(blocks_newdata)):
            # Storing First replica of block
            link = False
            while link is False:
                try:
                    # Storing block
                    link = self.dataserveradd[blocks[i]].put(path + str(i),blocks_newdata[i] + hashlib.md5(blocks_newdata[i]).hexdigest())
		    print("in write" + " newdata blocks" + str(blocks_newdata[i]) + "first copy" + str(self.dataserverport[blocks[i]]))
                except socket.error:
                    print('WRITE - link to Data Server at port ' + str(self.dataserverport[blocks[i]]) + ' is lost.')
                    sleep(5)

            # Store Second replica of block
            link = False
            while link is False:
                try:
                    # Storing block
                    link = self.dataserveradd[(blocks[i] + 1) % len(self.dataserverport)].put(path + str(i),blocks_newdata[i] + hashlib.md5(blocks_newdata[i]).hexdigest())
		    print("in write" + " newdata blocks" + str(blocks_newdata[i]) + "second copy" + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]))
                except socket.error:
                    print('WRITE - link to Data Server at port ' + str(self.dataserverport[(blocks[i] + 1) % len(self.dataserverport)]) + ' is lost.')
                    sleep(5)
	    # Store Third replica of block
            link = False
            while link is False:
                try:
                    # Storing block
                    link = self.dataserveradd[(blocks[i] + 2) % len(self.dataserverport)].put(path + str(i),blocks_newdata[i] + hashlib.md5(blocks_newdata[i]).hexdigest())
		    print("in write" + " newdata blocks" + str(blocks_newdata[i]) + "third copy" + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]))
                except socket.error:
                    print('WRITE - link to Data Server at port ' + str(self.dataserverport[(blocks[i] + 2) % len(self.dataserverport)]) + ' is lost.')
                    sleep(5)
	    link = False
            while link is False:
                try:
                 
                    link = self.dataserveradd[(blocks[i] + 3) % len(self.dataserverport)].get(path + str(i))
		    
                except socket.error:
                    print('WRITE - link to Data Server at port ' + str(self.dataserverport[(blocks[i] + 3) % len(self.dataserverport)]) + ' is lost.')
                    sleep(5) 

if __name__ == '__main__':
    if len(argv) < 6:
        print('usage: %s <mountpoint> <metaserver port> <dataserver port>' % argv[0])
        exit(1)
    metaserverport=int(argv[2])
    dataserverport=[]	
    for i in range(3,len(argv)):
	dataserverport.append(int(argv[i]))
	
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(metaserverport,dataserverport), argv[1], foreground=True, debug= True)
