# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 20:39:06 2016

@author: shenggao
"""

import os, multiprocessing, subprocess, urllib2
import logging
from multiprocessing import Semaphore


class VideoDownloader(object):
    #---get cpu count----
    MAX_CORE_NUMBER = multiprocessing.cpu_count() - 2
    BASE_DIR = os.path.dirname(__file__)
    
    #---init－－－－－－
    def __init__(self, output_path = 'videos', ffmpeg_path = 'ffmpeg/bin'):
        self._output_path = os.path.join(VideoDownloader.BASE_DIR, output_path)
        self._ffmepg_path = os.path.join(VideoDownloader.BASE_DIR, ffmpeg_path)
        self._thread_number = VideoDownloader.MAX_CORE_NUMBER
        pass
    
    #---config_logger－－－－－－
    def config_logger(self, log_name = 'logger_video_downloader'):
        self._logger = logging.getLogger("root")
        self._logger.setLevel(logging.warning)
        ch = logging.FileHandler(log_name)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s -%(message)s"))
        self._logger.addHandler(ch)
        pass
    
    #---get tasks from a csv file----
    def get_download_task_from_csv(self, csv_file_name):
        self._video_url = []
        with open(csv_file_name, 'rb') as fp:
            for line in fp:
                items = line.strip().split(',')
                assert(len(items) == 4)
                self._video_url.extend(items[-1])
            pass
        self._logger.info('Found %s urls in total' % len(self._video_url))
    
    def start_multiprocess(self, csv_file_name):
        self.get_download_task_from_csv(csv_file_name)
        semaphores = Semaphore(self._thread_number)
        for video_url in self.video_url:
            semaphores.acquire()
            multiprocessing.Process(target = self.run_downloader, args = (video_url, 0.3))
            semaphores.release()
        for idx in range(0, self._thread_number):
            semaphores.acquire()
        semaphores.release()
        pass

    def run_downloader(self, video_url, _MAX_VIDEO_DURATION_DIFF = 0.3):
        filename = video_url.split('/')[-1]
        self._logger.info("start download: %s"%filename)
        video_name = os.path.join(self._output_path, filename + '.mp4')
        duration_from_url = self.get_video_length_from_url(video_url)
        duration_from_file = self.get_video_length_from_file(video_name)
        self._logger.info('Duration: %0.3f from url, %0.3f from file'%(duration_from_url, duration_from_file))
        if not duration_from_url and not duration_from_file and abs(duration_from_url - duration_from_file) < _MAX_VIDEO_DURATION_DIFF:
            self._logger.infor("finished video file: %s"%(video_name))
        else:   
            self._logger.info("retry download video file: %s"%(video_name))
            command = '%s -v quiet -y -i %s -bsf:a aac_adtstoasc -vcodec copy -c copy -crf 50 %s/%s.mp4' % ( \
                    os.path.join(self._ffprobe_path, 'ffmpeg'), video_url, self._output_path, filename)
            self.run_cmd(command)
        pass
    
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
    
    def get_video_length_from_file(self, video_name, max_retry = 5):
        '''obtain video duration from file'''
        command = '%s -v quiet -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 %s' % \
                     (os.path.join(self._ffprobe_path, 'probe'), video_name)
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
        pass
    
    def get_shell_output(self, command, shell = False):
        return subprocess.check_output(command, shell)
    
if __name__ == '__main__':
    downloader = VideoDownloader()
    downloader.start_multiprocess('a.csv')