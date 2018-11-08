#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, CircularOrderedSet, util
from urllib.request import urlopen, Request
import gzip
import lxml.html
import traceback
import crawler_helper as rch
from lxml import etree
import datetime
from datetime import timezone
from collections import OrderedDict


class UserContentCrawler(Link):

    def setup(self):
        self.spider_name = rch.get_spider_name('UCE')
        self.last_queue = 6
        self.priorities = OrderedDict([
            (1, 86400),
            (2, 259200),
            (3, 604800),
            (4, 1209600),
            (5, 4838400)])

    def _priority_count(self, priority_counters, retrieved_posts,
                        start_timestamp, submission_timestamp):
        retrieved_posts[0] += 1
        # Ordered queues, if the timestamp matches a group, it
        # also matches the following ones
        and_following = False
        for priority, seconds in self.priorities.items():
            if and_following or start_timestamp \
            - submission_timestamp < seconds:
                if priority in priority_counters:
                    priority_counters[priority] = \
                        priority_counters[priority] + 1
                else:
                    priority_counters[priority] = 1
                and_following = True

    def transform(self, electron):
        """ The Electron instance contains the user_id in the value """
        start_timestamp = util.get_current_timestamp()

        electrons = []
        try:
            user_id = electron.value

            if not user_id:
                return

            # print('\n########### USUARIO: ' + user_id + ' ###########')
            # print("INPUT TOPIC ==> " + str(electron.topic))
            # print(rch._get_user_comments_url(user_id))
            # print(rch._get_user_submissions_url(user_id))

            queue_adder = 0
            queue_multiplier = 1

            try:
                _, probability = self.aerospike.get(user_id,
                                                    'test',
                                                    'aggregated_probabilities')
                if probability:
                    # The depression probability is inverted (higher queue
                    # number means less important)

                    # .0 => SAME PRIORITY
                    # .1 => SAME PRIORITY
                    # .2 => PRIORITY + 1
                    # .3 => PRIORITY + 1
                    # .4 = PRIORITY + 2
                    # .5 => MAX_PRIORITY
                    # .6 => MAX_PRIORITY
                    # .7 => MAX_PRIORITY
                    # .8 => MAX_PRIORITY
                    # .9 => MAX_PRIORITY
                    # 1. => MAX_PRIORITY

                    if probability < .4 and probability >= .2:
                        queue_adder = -1
                    elif probability < .5:
                        queue_adder = -2
                    # Big probabilities assign the first queue automatically
                    else:
                        queue_multiplier = 0
                        queue_adder = 1

                # LAST SUBMISSION FOR THE CURRENT USER
                _, last_submission = self.aerospike.get(user_id,
                                                        'test',
                                                        'last_submissions')

                # LAST COMMENT FOR THE CURRENT USER
                _, last_comment = self.aerospike.get(user_id,
                                                     'test',
                                                     'last_comments')

            except:
                util.print_fatal(self, "Unhandled exception...")

            retrieved_posts = [0]
            priority_counters = OrderedDict()

            # Retrieve the last 100 (max) submissions of the current user
            # Reverse order (older first)
            for element in rch.get_user_submissions_elements(self.spider_name,
                                                             user_id,
                                                             items_no=100)[::-1]:
                try:
                    submission_id = rch.get_submission_id(element)
                    subreddit_id = rch.get_subreddit_id(element)
                    submission_timestamp = rch.get_submission_timestamp(element)

                    self._priority_count(priority_counters, retrieved_posts,
                                         start_timestamp, submission_timestamp)

                    try:
                        submission = rch.get_submission_elements(self.spider_name,
                                                                 subreddit_id,
                                                                 submission_id)[0]
                    except IndexError:
                        # Sometimes the prefix 'u_' is needed for the username
                        submission = rch.get_submission_elements(self.spider_name,
                                                                 'u_' + subreddit_id,
                                                                 submission_id)[0]

                    submission_title = rch.get_submission_title(submission)
                    submission_body = rch.get_submission_body(submission)

                    # print('[title] ' + submission_title)
                    if submission_body:
                        pass
                        # print('[body] ' + submission_body)

                    # Avoid null submissions
                    if not last_submission \
                    or (submission_id and submission_id > last_submission):
                        last_submission = submission_id
                        electrons.append(Electron(
                            user_id,
                            {
                                'submission_id': submission_id,
                                'submission_title': submission_title,
                                'subreddit_id': subreddit_id,
                                'content': submission_body,
                                'type': 0,
                                'timestamp': submission_timestamp
                            },
                            topic=self.output_topics[0]))
                        self.aerospike.put(user_id,
                                                       last_submission,
                                                       'test',
                                                       'last_submissions')

                    else:
                        # If the current element is already stored, also
                        # the next ones
                        print("[IGNORED_SUBMISSION]")
                        break

                except IndexError:
                    # Missing subreddit (announcement?)
                    break

            # Retrieve the last 100 comments of the current user
            # Reverse order, older first
            for element in rch.get_user_comments_elements(self.spider_name,
                                                          user_id,
                                                          items_no=100)[::-1]:
                try:
                    comment_id = rch.get_comment_id(element)
                    comment_body = rch.get_comment_body(element)
                    comment_timestamp = rch.get_comment_timestamp(element)
                    submission_id = rch.get_comment_submission_id(element)
                    submission_title = rch.get_comment_submission_title(element)
                    subreddit_id = rch.get_comment_subreddit_id(element)

                    self._priority_count(priority_counters, retrieved_posts,
                                         start_timestamp, comment_timestamp)

                    # print('[comment] ' + comment_body)
                    # print(comment_timestamp)

                    # Avoid null comments
                    if not last_comment \
                    or (comment_id and comment_id > last_comment):
                        last_comment = comment_id
                        electrons.append(Electron(
                            user_id,
                            {
                                'comment_id': comment_id,
                                'submission_id': submission_id,
                                'submission_title': submission_title,
                                'subreddit_id': subreddit_id,
                                'content': comment_body,
                                'type': 1,
                                'timestamp': comment_timestamp
                            },
                            topic=self.output_topics[0]))
                        self.aerospike.put(user_id,
                                                       last_comment,
                                                       'test',
                                                       'last_comments')

                    else:
                        # If the current element is already stored, also
                        # the next ones
                        # print("[IGNORED_COMMENT]")
                        break

                except IndexError:
                    break

            # print("\n\t-- result --")
            # print("\t[*] retrieved: " + str(retrieved_posts))
            # print("\t[*] counters: " + str(priority_counters))

            # Only the new submissions/comments are taken into account
            # when exploring the user content. If there aren't enough
            # new texts, the classification is meaningless and the
            # previous queue is kept.
            if electron.previous_topic and retrieved_posts[0] < 10:
                queue = electron.previous_topic
            else:
                queue_number = self.last_queue
                for priority, count in priority_counters.items():
                    ratio = count / float(retrieved_posts[0])
                    # print("RATIO -> " + str(ratio))
                    if ratio > .2:
                        queue_number = priority
                        break

                queue_number = int(queue_number * queue_multiplier + queue_adder)
                # Force the first queue to be #1 despite out of range results
                if queue_number < 1:
                    queue_number = 1
                elif queue_number > self.last_queue:
                    queue_number = self.last_queue

                queue = 'p' + str(queue_number) + '_users'

            # print("\t[*] queue_multiplier: " + str(queue_multiplier))
            # print("\t[*] queue_adder: " + str(queue_adder))
            # print("\t[*] assigned_queue: " + queue)

            # The user will be returned to the new assigned queue
            electrons.append(Electron(
                None,
                user_id,
                topic=queue))

            return electrons

        except Exception:
            util.print_fatal(self, "Unhandled exception...")
            try:
                self.aerospike.close_connection()
            except Exception:
                pass
            raise SystemExit


if __name__ == "__main__":
    UserContentCrawler().start(link_mode=Link.MULTIPLE_KAFKA_INPUTS)
