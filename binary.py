import subprocess
import os
import signal

def run_command(n):
    process = subprocess.Popen(["./command-line.exe", "small_filename.dat",str(n),"1KB","64B","app_binary_test.log"])
    process.wait()
    if process.returncode<0:
        return True
    else:
        return False

l,h=0,10000000

while l <=h:
    m=(l+h)//2
    if run_command(m):
        h=m+1
    else:
        l=m-1
m=(l+h)//2

print("threshold: ",m)