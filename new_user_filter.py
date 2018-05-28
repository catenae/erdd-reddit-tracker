#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, util
from pymongo import MongoClient
from conf import conf_loader as conf


class NewUserFilter(Link):

    def setup(self):
        self.mongo_client = MongoClient(conf.mongo['address'], conf.mongo['port'])
        self.db = self.mongo_client.reddit_early_risk
        self.users = self.db.users

        # Ensure nickname index
        self.users.create_index('nickname', unique=True, background=True)

    def transform(self, electron):
        try:
            if not self.aerospike.exists(
                electron.value,
                'test',
                'known_users'):

                # print(f"[ACCEPTED] ({electron.value})")
                self.aerospike.put(electron.value,
                                               None,
                                               'test',
                                               'known_users')
                self.users.update_one(
                    {'nickname': electron.value},
                    {'$set': {'nickname': electron.value}},
                    upsert=True)

                return electron

            else:
                pass
                # print(f"[IGNORED] ({electron.value})")

        except Exception as e:
            self.aerospike.close_connection()
            self.mongo_client.close()
            util.print_exception(self,
                f"Unhandled exception. Value: {electron.value}Exiting...",
                fatal=True)

if __name__ == "__main__":
    NewUserFilter().start()
