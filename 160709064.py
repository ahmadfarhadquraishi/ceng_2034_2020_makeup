#!/usr/bin/python3
#ahmad farhad quraishi
import os
import requests
import threading
import hashlib
import uuid
import time
import multiprocessing
import psutil
import math 

def hashFiles(fileNames, q):
  # Uncomment the following 2 lines to test behaviour if process timesout
  # if len(fileNames) > 1:
  #   time.sleep(31)
  for fileName in fileNames:
    md5_hash = hashlib.md5()
    a_file = open(fileName, "rb")
    content = a_file.read()
    md5_hash.update(content)
    digest = md5_hash.hexdigest()
    q.put([fileName, digest])
  
def getDups(names, q=None, retry=False):
  files=len(names)
  cores=multiprocessing.cpu_count()
  print("Number of files to hash:",files)
  print("Cpu count:",cores)
  filesForEachCore=math.floor(files/cores)
  remainingTasks = files - (filesForEachCore * cores);
  if remainingTasks > 0:
    print(remainingTasks,"core"+ ("s" if remainingTasks>1 else "") +" will be assigned",filesForEachCore+1,"files to hash")
  print(cores-remainingTasks,"core"+("s" if cores-remainingTasks>1 else "")+" will be assigned",filesForEachCore,"files to hash")
  print()
  hashes=[]
  processes=[]
  if q==None:
   q = multiprocessing.Queue()
  for i in range(cores):
    filesToHash=filesForEachCore
    if i < remainingTasks:
      filesToHash=filesToHash+1
    namesToHash=list(names.keys())[i:i+filesToHash]
    print("Core #"+str(i+1), " is created and will hash", filesToHash, "files:")
    print(namesToHash)
    print()
    p = multiprocessing.Process(target=hashFiles,args=(namesToHash, q));
    p.names=namesToHash
    p.start()
    processes.append(p)
  print()
    


  #waiting for all processes to finish, for 30 seconds
  timeout=30
  while True:
     onealive=False
     for p in processes:
       if p.is_alive():
         onealive=True
         break
     if not onealive:
       break
     elif timeout <= 0: #at least one is live but time finished so restart job
       #remove all finished processes
       newNames={}
       for i in range(len(processes)):
         if processes[i].is_alive():
           print("Hashing process failed and will be repeated for:")
           for name in processes[i].names:
             print(names[name])
           newNames[name]=names[name]
           print()
       #Start again
       getDups(newNames, q, True)
       break
     else:
       timeout=timeout-1
       time.sleep(1)
  if retry==True:  #If it's a retry just put items in the queue and end
    return
  #if it's main function then get items from queue and return
  hashes={}
  dupNames=[]
  for n in names:
    item=q.get()
    for hashstr in hashes:
      if hashstr==item[1]:
        dupNames.append([item[0], hashes[hashstr]])
        continue
    hashes[item[1]]=item[0]
  return dupNames
       

def childFunction():
  urls=["http://wiki.netseclab.mu.edu.tr/images/thumb/f/f7/MSKU-BlockchainResearchGroup.jpeg/300px-MSKU-BlockchainResearchGroup.jpeg","https://upload.wikimedia.org/wikipedia/tr/9/98/Mu%C4%9Fla_S%C4%B1tk%C4%B1_Ko%C3%A7man_%C3%9Cniversitesi_logo.png","https://upload.wikimedia.org/wikipedia/commons/thumb/c/c3/Hawai%27i.jpg/1024px-Hawai%27i.jpg","http://wiki.netseclab.mu.edu.tr/images/thumb/f/f7/MSKU-BlockchainResearchGroup.jpeg/300px-MSKU-BlockchainResearchGroup.jpeg","https://upload.wikimedia.org/wikipedia/tr/9/98/Mu%C4%9Fla_S%C4%B1tk%C4%B1_Ko%C3%A7man_%C3%9Cniversitesi_logo.png"]
  #Downloading files in multiple threads, waiting for all to finish
  threads=[]
  names={}
  print("Downloading files in multiple threads");
  for url in urls:
    name=str(uuid.uuid4())
    names[name]=url
    print("downloading " + url)
    threads.append(threading.Thread(target=download_file, args=(url, name)))
  #Start all threads and after starting all, start joining so they can all run at the same time instead of sequentially if joining right after starting each one
  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join()
  print("FINISHED DOWNLOADING FILES\n")
  
  print("Hashing files in multiple processes")
  duplicates = getDups(names)
  print("FINISHED HASHING FILES\n")
  for dup in duplicates:
    print("DUPLICATE:",names[dup[0]],"\nWITH:\t",names[dup[1]],"\n")
  print("DELETING FILES")
  for name in names:
    print("DELETING:", name)
    os.remove(name)


def download_file(url, file_name=None):
    r = requests.get(url, allow_redirects=True)
    file = file_name if file_name else str(uuid.uuid4())
    open(file, 'wb').write(r.content)
    return r.content
   


def proc_c():
    t = os.fork()
    if t>0:
        print("parent PROC ID is:",os.getpid())
    else:
        print("child PROC ID is:",os.getpid(),"\n")
        childFunction()        


def isEnoughMemory():
  minimum = 10 * 1024 * 1024  # 10MB
  stats = psutil.virtual_memory()  # returns a named tuple
  if stats.available < minimum:
    return False
  else:
    return True;


while not isEnoughMemory():
  print("Not enough memory, retrying in 10 seconds...")
  time.sleep(10)
proc_c()