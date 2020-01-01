#!/usr/bin/env python

"""
remove_topics.py
The tool to remove topics from ROS bags.
"""

__version__     = "1.0.0"
__author__      = "David Qiu"
__email__       = "david@davidqiu.com"
__website__     = "www.davidqiu.com"
__copyright__   = "Copyright (C) 2019, Dicong Qiu."
__license__     = "MIT"


import os, argparse
import rosbag
from tqdm import tqdm


def remove_topics_from_bag(path_bag, topics, path_outbag, progress=False):
    """
    Remove topics from a ROS bag.

    @param path_bag The path to the ROS bag.
    @param topics List of names of the topics to remove.
    @param path_outbag The path to output the processed ROS bag.
    @param progress Whether fo display progress bar.
    @return n_msgs The number of messages exported.
    """

    n_msgs = 0

    # open bag files
    bag = rosbag.Bag(path_bag)
    outbag = rosbag.Bag(path_outbag, mode='w')

    # get ttinfo of bag
    ttinfo = bag.get_type_and_topic_info()

    # determine topics to keep
    topics_remain = []
    for topic_name in ttinfo.topics:
        if topic_name not in topics:
            topics_remain.append(topic_name)

    # initialize progress bar
    if progress:
        total = 0
        for topic_name in topics_remain:
            total += ttinfo.topics[topic_name].message_count
        pbar = tqdm(
            desc  = 'Exporting',
            total = total,
            unit  = 'msg',
        )

    # read messages from the bag file
    for (topic, msg, t) in bag.read_messages(topics=list(topics_remain)):
        # write message to output bag file
        outbag.write(topic, msg, t=t)

        # update progress bar
        if progress:
            pbar.update(1)

    # close bag files
    bag.close()
    outbag.close()


def parse_arguments():
    """
    Parse arguments.

    @return The object of parsed arguments.
    """

    parser = argparse.ArgumentParser(
        description='Pick ROS bag topics.'
    )

    parser.add_argument(
        '-b', '--bags', nargs='+', required=True, type=str,
        help='path to the bag files to process'
    )

    parser.add_argument(
        '-t', '--topics', nargs='+', required=True, type=str,
        help='names of the topics to remove'
    )

    parser.add_argument(
        '--postfix', required=False, type=str, default='.filtered',
        help='postfix to add to the output processed bag files'
    )

    parser.add_argument(
        '--outdir', required=False, type=str, default=None,
        help='path to the directory to output the processed bag files'
    )

    args = parser.parse_args()

    return args


def main():
    """
    Main entry.
    """

    args = parse_arguments()

    for path_bag in args.bags:
        # determine output directory
        path_outdir = args.outdir
        if path_outdir is None:
            path_outdir = os.path.dirname(path_bag)

        # determine path to output the processed bag file
        path_outbag = os.path.join(
            path_outdir,
            os.path.splitext(os.path.basename(path_bag))[0] +
                args.postfix + '.bag'
        )

        print('Processing "%s" -> "%s"' % (path_bag, path_outbag))
        remove_topics_from_bag(
            path_bag, args.topics, path_outbag, progress=True
        )


if __name__ == "__main__":
    main()
