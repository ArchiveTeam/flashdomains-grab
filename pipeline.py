# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import io
import os.path
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter

if StrictVersion(seesaw.__version__) < StrictVersion('0.8.5'):
    raise Exception('This pipeline needs seesaw version 0.8.5 or higher.')


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_AT will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string

WGET_AT = find_executable(
    'Wget+AT',
    [
        'GNU Wget 1.20.3-at.20200919.01',
        'GNU Wget 1.20.3-at.20201030.01'
    ],
    ['./wget-at']
)

if not WGET_AT:
    raise Exception('No usable Wget+At found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20201231.01'
USER_AGENT = 'Archive Team'
TRACKER_ID = 'domains-flash'
TRACKER_HOST = 'trackerproxy.archiveteam.org'


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckIP')
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 6:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, 'PrepareDirectories')
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item['item_name']
        escaped_item_name = item_name.replace(':', '_').replace('/', '_').replace('~', '_')
        dirname = '/'.join((item['data_dir'], escaped_item_name[:30]))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item['item_dir'] = dirname
        item['warc_file_base'] = '-'.join([
            self.warc_prefix,
            #escaped_item_name[:45],
            hashlib.sha1(item_name.encode('utf8')).hexdigest(),
            time.strftime('%Y%m%d-%H%M%S')
        ])

        open('%(item_dir)s/%(warc_file_base)s.warc.gz' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_data.txt' % item, 'w').close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'MoveFiles')

    def process(self, item):
        os.rename('%(item_dir)s/%(warc_file_base)s.warc.gz' % item,
              '%(data_dir)s/%(warc_file_base)s.warc.gz' % item)
        os.rename('%(item_dir)s/%(warc_file_base)s_data.txt' % item,
              '%(data_dir)s/%(warc_file_base)s_data.txt' % item)

        has_metadata = False

        with open('%(data_dir)s/%(warc_file_base)s.warc.gz' % item, 'rb') as f:
            for record in ArchiveIterator(f):
                if record.rec_type == 'warcinfo':
                    info_id = record.rec_headers.get_header('WARC-Record-ID')
                    for l in record.content_stream().read().split(b'\r\n'):
                        if l.startswith(b'wget-arguments'):
                            wget_arguments = l.split(b':', 1)[1].strip()
                if record.rec_type == 'resource':
                    has_metadata = True

        if not has_metadata:
            with open('%(data_dir)s/%(warc_file_base)s-tail.warc.gz' % item, 'wb') as f:
                writer = WARCWriter(f, gzip=True)
                record = writer.create_warc_record(
                    'metadata://gnu.org/software/wget/warc/MANIFEST.txt',
                    'resource',
                    payload=io.BytesIO(bytes(info_id, 'utf8')+b'\n'),
                    warc_headers_dict={
                        'WARC-Warcinfo-ID': info_id,
                        'Content-Type': 'text/plain'
                    }
                )
                manifest_id = record.rec_headers.get_header('WARC-Record-ID')
                writer.write_record(record)
                record = writer.create_warc_record(
                    'metadata://gnu.org/software/wget/warc/wget_arguments.txt',
                    'resource',
                    payload=io.BytesIO(wget_arguments+b'\n'),
                    warc_headers_dict={
                        'WARC-Warcinfo-ID': info_id,
                        'WARC-Concurrent-To': manifest_id,
                        'Content-Type': 'text/plain'
                    }
                )
                writer.write_record(record)
                with open('%(item_dir)s/wget.log' % item, 'rb') as f_log:
                    record = writer.create_warc_record(
                        'metadata://gnu.org/software/wget/warc/wget.log',
                        'resource',
                        payload=f_log,
                        warc_headers_dict={
                            'WARC-Warcinfo-ID': info_id,
                            'WARC-Concurrent-To': manifest_id,
                            'Content-Type': 'text/plain'
                        }
                    )
                writer.write_record(record)
        else:
            open('%(data_dir)s/%(warc_file_base)s-tail.warc.gz' % item, 'w').close()

        shutil.rmtree('%(item_dir)s' % item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()

CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'domains.lua'))

def stats_id_function(item):
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_AT,
            '-U', USER_AGENT,
            '-nv',
            '--content-on-error',
            '--lua-script', 'domains.lua',
            '-o', ItemInterpolation('%(item_dir)s/wget.log'),
            '--no-check-certificate',
            '--output-document', ItemInterpolation('%(item_dir)s/wget.tmp'),
            '--truncate-output',
            '-e', 'robots=off',
            '--rotate-dns',
            '--recursive', '--level=inf',
            '--no-parent',
            '--page-requisites',
            '--timeout', '30',
            '--tries', 'inf',
            '--span-hosts',
            '--waitretry', '30',
            '--warc-file', ItemInterpolation('%(item_dir)s/%(warc_file_base)s'),
            '--warc-header', 'operator: Archive Team',
            '--warc-header', 'domains-dld-script-version: ' + VERSION,
            '--warc-header', ItemInterpolation('domains-item: %(item_name)s'),
            '--warc-dedup-url-agnostic',
        ]

        item_name = item['item_name']

        wget_args.extend(['--domains', item_name])
        wget_args.extend(['--warc-header', 'domain: ' + item_name])

        wget_args.append('http://{}/'.format(item_name))
        wget_args.append('https://{}/'.format(item_name))

        if item_name.count('.') == 1:
            wget_args.append('http://www.{}/'.format(item_name))
            wget_args.append('https://www.{}/'.format(item_name))

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title = 'domains',
    project_html = '''
    <img class="project-logo" alt="logo" src="https://archiveteam.org/images/0/06/Adobe-swf-icon.png" height="50px"/>
    <h2>domains <span class="links"><a href="https://archiveteam.org/">Website</a> &middot; <a href="http://tracker.archiveteam.org/domains/">Leaderboard</a></span></h2>
    '''
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker('http://%s/%s' % (TRACKER_HOST, TRACKER_ID), downloader,
        VERSION),
    PrepareDirectories(warc_prefix='domains'),
    WgetDownload(
        WgetArgs(),
        max_tries=1,
        accept_on_exit_code=[-6, 0, 4, 8],
        env={
            'max_seconds': str(30*24*3600),
            'max_urls': '1000000',
            'max_bytes': str(400*1024**3)
        }
    ),
    PrepareStatsForTracker(
        defaults={'downloader': downloader, 'version': VERSION},
        file_groups={
            'data': [
                ItemInterpolation('%(item_dir)s/%(warc_file_base)s.warc.gz')
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default='2',
        name='shared:rsync_threads', title='Rsync threads',
        description='The maximum number of concurrent uploads.'),
        UploadWithTracker(
            'http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s.warc.gz'),
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s-tail.warc.gz'),
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s_data.txt')
            ],
            rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
            rsync_extra_args=[
                '--recursive',
                '--partial',
                '--partial-dir', '.rsync-tmp',
                '--min-size', '1',
                '--no-compress',
                '--compress-level', '0'
            ]
        ),
    ),
    SendDoneToTracker(
        tracker_url='http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)
