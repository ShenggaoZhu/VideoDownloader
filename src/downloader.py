# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 20:39:06 2016

@author: shenggao
"""

import os, multiprocessing, subprocess, urllib2
import logging
from multiprocessing import Semaphore
from glob import glob


class VideoDownloader(object):
    #---get cpu count----
    MAX_CORE_NUMBER = multiprocessing.cpu_count() - 2
    BASE_DIR = os.path.dirname(__file__)
    
    #---init－－－－－－
    def __init__(self, output_path = 'videos', ffmpeg_path = 'ffmpeg/bin'):
        self._output_path = output_path
        self._ffmepg_path = os.path.abspath(ffmpeg_path)
        self._thread_number = VideoDownloader.MAX_CORE_NUMBER
        self._MAX_VIDEO_DURATION_DIFF = 0.5
        
        self.config_logger()
    
    #---config_logger－－－－－－
    def config_logger(self, log_name = 'video_downloader.log'):
        self._logger = logging.getLogger("root")
        self._logger.setLevel(logging.DEBUG)
        ch = logging.FileHandler(log_name)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s -%(message)s"))
        self._logger.addHandler(ch)
        
        ch = logging.StreamHandler() # console
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("%(levelname)s -%(message)s"))
        self._logger.addHandler(ch)
    
    def __getstate__(self):
        'logger object cannot be pickled in multiprocessing...'
        d = self.__dict__.copy()
        del d['_logger']
        return d
    
    def __setstate__(self, d):
        self.__dict__.update(d)
        self.config_logger()
        
    
    #---get tasks from a csv file----
    def get_download_task_from_csv(self, csv_file_name):
        self._video_url = []
        with open(csv_file_name, 'rb') as fp:
            for line in fp:
                items = line.strip().split(',')
#                assert(len(items) == 4)
                if len(items) == 4: # fail silently
                    self._video_url.append(items[-1])
            pass
        self._logger.info('Found %s urls in total' % len(self._video_url))
    
    def start_multiprocess(self, csv_file_name):
        self.get_download_task_from_csv(csv_file_name)
        semaphores = Semaphore(self._thread_number)
        for video_url in self._video_url:
            semaphores.acquire()
            process = multiprocessing.Process(target = self.run_downloader, args = (semaphores, video_url, self._MAX_VIDEO_DURATION_DIFF))
            process.daemon = True
            process.start()
#            semaphores.release()
        for _ in range(0, self._thread_number):
            semaphores.acquire()
        semaphores.release()
    
    def download_all_csv_files(self):
        'download all playback csv files one by one'
        csv_files = glob('*playback*.csv')
        self._logger.info('Found %s playback csv files' % len(csv_files))
        for csv_file_name in csv_files:
            self._logger.info('Start downloading csv file: %s' % csv_file_name)
            self.start_multiprocess(csv_file_name)

    def run_downloader(self, semaphores, video_url, _MAX_VIDEO_DURATION_DIFF):
        filename = video_url.split('/')[-1]
        self._logger.info("start download: %s"%filename)
        video_name = os.path.join(self._output_path, filename + '.mp4')
        if os.path.exists(video_name): #  check duration only if video exists
            duration_from_url = self.get_video_length_from_url(video_url)
            duration_from_file = self.get_video_length_from_file(video_name)
            self._logger.info('Duration: %s from url, %s from file'%(duration_from_url, duration_from_file))
            if duration_from_url is not None and duration_from_file is not None and \
                abs(duration_from_url - duration_from_file) < _MAX_VIDEO_DURATION_DIFF:
                self._logger.info("finished video file: %s"%(video_name))
                semaphores.release()
                return
            else:   
                self._logger.info("retry download video file: %s"%(video_name))
        command = '"%s" -v quiet -y -i %s -bsf:a aac_adtstoasc -vcodec copy -c copy -crf 50 %s/%s.mp4' % ( \
                os.path.join(self._ffmepg_path, 'ffmpeg'), video_url, self._output_path, filename)
        self.run_shell(command)
        semaphores.release()
    
    def get_video_length_from_url(self, video_url, max_retry = 5):
        '''obtain video duration from url'''
        for idx in range(0, max_retry):
            try:
                response = urllib2.urlopen(video_url)
                data = response.read()
                duration = 0
                for line in data.splitlines():
                    duration = duration + float(line.split(':')[1].split(',')[0].strip()) if '#EXTINF' in line else duration
                break
            except:
                self._logger.warning("reading video_url: %s, recheck %d"%(video_url, idx+1))
                duration = None
                pass
        return duration
    
    def get_video_length_from_file(self, video_name, max_retry = 2):
        '''obtain video duration from file'''
        command = '"%s" -v quiet -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 %s' % \
                     (os.path.join(self._ffmepg_path, 'ffprobe'), video_name)
        for idx in range(0, max_retry):
            try:
                result = self.get_shell_output(command, shell=True)
                duration = float(result.strip())
                break
            except:
                self._logger.warning("reading video_name: %s, recheck %d"%(video_name, idx+1))
                duration = None
                pass
        return duration
                

    def run_shell(self, command):
        subprocess.call(command, shell=True)
    
    def get_shell_output(self, command, shell = False):
        return subprocess.check_output(command, shell)
    
if __name__ == '__main__':
    downloader = VideoDownloader()
    downloader.download_all_csv_files()
