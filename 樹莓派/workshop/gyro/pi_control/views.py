from django.shortcuts import render

# Create your views here.

from django.shortcuts import render
from django.http import HttpResponse
import threading
import time,os
from picamera import PiCamera
from time import sleep


# Create your views here.
run_station = True
ID = 'VG13213258'
os.system('mkdir -p ./picture/'+ID+'/waiting/front/')
os.system('mkdir -p ./picture/'+ID+'/working/front/')
os.system('mkdir -p ./picture/'+ID+'/error/front/')
os.system('mkdir -p ./picture/'+ID+'/finish/front/')  
os.system('mkdir -p ./picture/'+ID+'/origin/front/')

def post_list(request):
    return HttpResponse("Hello, world. You're at the polls index.")

def pi_print(request):
    print("this is a function")
    return HttpResponse("Hello, world. You're at the polls index.")

def start_run_run(self):
    global run_station
    t = threading.Thread(target=rab_picture)
    time.sleep(1)
    run_station = True
    t.start()
    return HttpResponse("start run")

def stop_run_run(self):
    global run_station
    run_station = False
    return HttpResponse("stop run")


def rab_picture():#前端鏡頭

    i=0
    camera = PiCamera()
    camera.resolution =(800,600)
    camera.framerate =60    
    while True:
        if run_station == False:
            break
        else:
            camera.start_preview()
            camera.capture('./picture/'+ID+'/waiting/front/'+str(i)+'.png')
            i += 1
            time.sleep(2)  
        
    camera.stop_preview()
    os.system('scp -rp picture weiclass@10.120.28.50:/home/weiclass/Tibame_project/openpose/')
    print('done')
