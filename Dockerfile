#!/bin/bash

# Catenae Link
# Copyright (C) 2017-2018 Rodrigo Martínez <dev@brunneis.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

FROM catenae/link:0.1.0

RUN \
    pip install --upgrade pip \
    && pip install pymongo

# Topology links
COPY \
    crawler_helper.py \
    submission_crawler.py \
    comment_crawler.py \
    new_user_filter.py \
    user_content_crawler.py \
    post_storer.py /opt/reddit-depression/user-tracker/

# Configuration files
COPY conf /opt/reddit-depression/user-tracker/conf/

COPY entrypoint.sh /
ENTRYPOINT ["sh", "/entrypoint.sh"]
