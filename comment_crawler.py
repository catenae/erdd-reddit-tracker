#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, CircularOrderedSet
from urllib.request import urlopen, Request
import gzip
import lxml.html
import random
import traceback
import time
import crawler_helper as rch


class CommentCrawler(Link):

    def setup(self):
        self.spider_name = rch.get_spider_name('RCC')
        self.user_buffer_set = CircularOrderedSet(100)
        self.wait_seconds = 3 # Max waiting seconds between loops

    def _emit_retrieved(self, user_id):
        if user_id not in self.user_buffer_set:
            self.user_buffer_set.add(user_id)
            self.send(Electron(user_id,
                               user_id,
                               topic=self.output_topics[0]))

    def custom_input(self):
        running = True
        while(running):
            for element in rch.get_all_comments_elements(self.spider_name,
                                                         items_no=100):
                user_id = rch.get_user_id_from_subreddit(element)
                self._emit_retrieved(user_id)
            time.sleep(random.uniform(0, self.wait_seconds))


if __name__ == "__main__":
    CommentCrawler().start(link_mode=Link.CUSTOM_INPUT)
