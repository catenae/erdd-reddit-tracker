#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, util
from pymongo import MongoClient
from conf import conf_loader as conf


class PostStorer(Link):

    def setup(self):
        self.mongo_client = MongoClient(conf.mongo['address'], conf.mongo['port'])
        self.db = self.mongo_client.reddit_early_risk
        self.submissions = self.db.submissions
        self.comments = self.db.comments

        # Ensure submissions indices
        self.submissions.create_index('submission_id', unique=True, background=True)
        self.submissions.create_index('author', unique=False, background=True)
        self.submissions.create_index('subreddit_id', unique=False, background=True)
        self.submissions.create_index('timestamp', unique=False, background=True)

        # Ensure comments indices
        self.comments.create_index('comment_id', unique=True, background=True)
        self.comments.create_index('author', unique=False, background=True)
        self.comments.create_index('subreddit_id', unique=False, background=True)
        self.comments.create_index('timestamp', unique=False, background=True)


    def transform(self, electron):
        try:
            # Common attributes which will be stored for both submissions
            # and comments
            post = {'author': electron.key,
                    'submission_id': electron.value['submission_id'],
                    'subreddit_id': electron.value['subreddit_id'],
                    'submission_title': electron.value['submission_title'],
                    'content': electron.value['content'],
                    'timestamp': electron.value['timestamp']}

            if electron.value['type'] == 0:
                self.submissions.update_one(
                    {'submission_id': post['submission_id']},
                    {'$set': post},
                    upsert=True)

            elif electron.value['type'] == 1:
                post['comment_id'] = electron.value['comment_id']
                self.comments.update_one(
                    {'comment_id': post['comment_id']},
                    {'$set': post},
                    upsert=True)

        except:
            self.mongo_client.close()
            util.print_exception(self,
                f"Unhandled exception. Value: {electron.value}Exiting...",
                fatal=True)

if __name__ == "__main__":
    PostStorer().start(link_mode=Link.CUSTOM_OUTPUT)
