import json
import time
import urllib
import urllib.parse
import urllib.request
import logging
import os

from concurrent import futures
from urllib.request import urlopen
from zipfile import ZipFile
from concurrent.futures import as_completed


API_KEY = 'XXX'
ENDPOINT = 'https://api.amplitude.com/httpapi'



# subclass of ThreadPoolExecutor that provides:
#   - proper exception logging from futures
#   - ability to track futures and get results of all submitted futures
#   - failure on exit if a future throws an exception (when futures are tracked)
class Executor(futures.ThreadPoolExecutor):

	def __init__(self, max_workers):
		super(Executor, self).__init__(max_workers)
		self.track_futures = False
		self.futures = []

	def submit(self, fn, *args, **kwargs):
		def wrapped_fn(args, kwargs):
			return fn(*args, **kwargs)
		future = super(Executor, self).submit(wrapped_fn, args, kwargs)
		if self.track_futures:
			self.futures.append(future)
		return future

	def results(self):
		if not self.track_futures:
			raise Exception('Cannot get results from executor without future tracking')
		return (future.result() for future in as_completed(self.futures))

	def __enter__(self):
		self.track_futures = True
		return super(Executor, self).__enter__()

	def __exit__(self, exc_type, exc_val, exc_tb):
		try:
			for future in as_completed(self.futures):
				future.result()
			self.futures = []
			self.shutdown(wait=False)
			return False
		finally:
			super(Executor, self).__exit__(exc_type, exc_val, exc_tb)


def run_with_retry(f, row, tries, failure_callback=None):
	while True:
		try:
			return f()
		except Exception as e:
			tries -= 1
			if tries <= 0:
				logging.info('[%s] Failed to run %s Encountered %s (0 tries left, giving up)', os.getpid(), f, e.__class__.__name__)
				break
			else:
				if failure_callback:
					failure_callback()
				logging.info(
					'In Row [%s] in PID [%s] Raised %s, retrying (%s tries left)',
					row, os.getpid(), str(e), tries)


def send_req(json_events, row):
	data = urllib.parse.urlencode({'api_key': API_KEY, 'event': json_events})
	url = ENDPOINT + '?' + data

	def do_send():
		req = urllib.request.Request(url)
		response = urlopen(req, timeout=60)
		response.read()
		response.close()
		if response.code != 200:
			raise Exception('Bad response: ' + response.code)
	run_with_retry(do_send, row, tries=10, failure_callback=lambda: time.sleep(10))

def upload(events, rownum):
	start = time.time()
	with Executor(max_workers=16) as executor:
		for i in range(0, len(events), 10):
			executor.submit(send_req, json.dumps(events[i:i + 10]), rownum + i)
	diff = time.time() - start
	logging.info('uploading %s events took %s', len(events), diff)

def main():
	import sys

	rootLogger = logging.getLogger()
	rootLogger.setLevel(logging.DEBUG)

	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(logging.INFO)
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	ch.setFormatter(formatter)
	rootLogger.addHandler(ch)
	logging.info('start event import')

	if API_KEY == 'YOUR_API_KEY':
		logging.info('Must set API_KEY')
		return

	filename = sys.argv[1]
	start = int(sys.argv[2])

	rownum = 0
	cur_events = []
	zf = ZipFile(filename, 'r', allowZip64=True)
	with zf.open(zf.infolist()[0]) as f:
		for line in f:
			rownum += 1
			if rownum <= start:
				continue
			event = json.loads(line.strip())
			if (
				'event_type' not in event or
					('user_id' not in event and 'device_id' not in event)
			):
				continue
			cur_events.append(event)
			if len(cur_events) >= 100:
				logging.info('uploading %s events, row %s', len(cur_events), rownum)
				upload(cur_events, rownum)
				time.sleep(1)
				cur_events = []
		if cur_events:
			logging.info('uploading %s events, row %s', len(cur_events), rownum)
			upload(cur_events, rownum)


if __name__ == '__main__':
	main()
