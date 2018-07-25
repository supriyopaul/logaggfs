# logaggfs
fuse file system for logging

# usage
python test.py --log-file log --log-cache-dir ~/supriyo/logcache/ ~/supriyo/mountpoint/ -f

#output
root@ubuntu-2gb-nbg1-1 ~/supriyo # tree logcache/
logcache/
├── logs
├── mirror
│   ├── blah
│   └── blahdir
└── state

3 directories, 2 files
root@ubuntu-2gb-nbg1-1 ~/supriyo # tree mountpoint/
mountpoint/


