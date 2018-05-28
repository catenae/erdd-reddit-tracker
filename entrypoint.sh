#!/bin/bash

LINKS_PATH=/opt/reddit-depression/user-tracker
cd $LINKS_PATH

case "$1" in
    submission_crawler)
        LINK=submission_crawler.py
        ;;

    comment_crawler)
        LINK=comment_crawler.py
        ;;

    new_user_filter)
        LINK=new_user_filter.py
        ;;

    user_content_crawler)
        LINK=user_content_crawler.py
        ;;

    post_storer)
        LINK=post_storer.py
        ;;

    *)
        echo "Usage: [submission_crawler | comment_crawler | new_user_filter |"
        echo "        user_content_crawler | post_storer] [ARGS]"
        exit 1
esac

shift
python $LINK "$@"
