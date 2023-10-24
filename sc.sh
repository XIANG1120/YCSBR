#!/bin/bash
git add *
git commit -m "锁改为指针"
git push origin master
git tag -d v2.3
git push --delete origin v2.3
git tag -a v2.3 -m "锁改为指针"
git push origin v2.3
