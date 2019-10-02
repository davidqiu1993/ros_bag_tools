#!/usr/bin/env python

"""
export_csv.py
The tool to export topics from ROS bags to csv format files.
"""

__version__     = "1.0.0"
__author__      = "David Qiu"
__email__       = "david@davidqiu.com"
__website__     = "www.davidqiu.com"
__copyright__   = "Copyright (C) 2019, Dicong Qiu."
__license__     = "MIT"


import os, argparse
import rosbag
import csv
from tqdm import tqdm


def flatten_topic_fields(msg, field_prefix=''):
    """
    Flatten the fields of a topic.

    @param msg A message for topic.
    @param field_prefix The prefix of the current layer of topic fields.
    @return fieldnames The flattened topic fields.
    """

    assert(hasattr(msg, '__slots__'))

    fieldnames = []

    for s in msg.__slots__:
        fieldname = '.'.join([field_prefix, s]) if field_prefix else s
        if not hasattr(msg.__getattribute__(s), '__slots__'):
            fieldnames.append(fieldname)
        else:
            fieldnames += flatten_topic_fields(msg.__getattribute__(s), field_prefix=fieldname)

    return fieldnames


def get_field_value(msg, fieldname):
    """
    Get the value of a field from a topic message.

    @param msg The topic message.
    @param fieldname The name of the field to get the value from.
    @return value The value of the field.
    """

    layer = msg
    for layer_name in fieldname.split('.'):
        layer = layer.__getattribute__(layer_name)

    return layer


def export_topic_to_csv(path_bag, topic_name, path_csv, progress=False):
    """
    Export a topic from a ROS bag to a csv format file.

    @param path_bag The path to the ROS bag.
    @param topic_name The name of the topic to export.
    @param path_csv The path to the csv file to export to.
    @param progress Whether fo display progress bar.
    @return n_msgs The number of messages exported.
    """

    n_msgs = 0
    fieldnames = None
    csv_file = None
    csv_writer = None

    # open bag file
    bag = rosbag.Bag(path_bag)

    # initialize progress bar
    if progress:
        ttinfo = bag.get_type_and_topic_info()
        pbar = tqdm(
            desc  = topic_name,
            total = ttinfo.topics[topic_name].message_count,
            unit  = 'msg',
        )

    # read messages from the bag file
    for (topic, msg, t) in bag.read_messages(topics=list([topic_name])):
        # flatten topic fields and open output csv file if necessary
        if fieldnames is None:
            # flatten the topic fields
            fieldnames = flatten_topic_fields(msg)

            # open csv file
            csv_file = open(path_csv, 'w')
            csv_writer = csv.DictWriter(
                csv_file, fieldnames=(['_ros_time_sec'] + fieldnames)
            )
            csv_writer.writeheader()

        # extract values from the message and write to the csv file
        row = {}
        row['_ros_time_sec'] = t.to_sec()
        for fieldname in fieldnames:
            row[fieldname] = get_field_value(msg, fieldname)
        csv_writer.writerow(row)

        # update progress bar
        if progress:
            pbar.update(1)

    # close the csv file
    csv_file.close()


def parse_arguments():
    """
    Parse arguments.

    @return The object of parsed arguments.
    """

    parser = argparse.ArgumentParser(
        description='Export ROS bag topic to csv format.'
    )

    parser.add_argument(
        '-b', '--bags', nargs='+', required=True, type=str,
        help='path to the bag files'
    )

    parser.add_argument(
        '-t', '--topics', nargs='+', required=True, type=str,
        help='names of the topics to export'
    )

    parser.add_argument(
        '-o', '--outdir', required=True, type=str,
        help='path to directory to output the csv files'
    )

    args = parser.parse_args()

    return args


def main():
    """
    Main entry.
    """

    args = parse_arguments()

    for path_bag in args.bags:
        for topic_name in args.topics:
            # construct csv file name prefix
            fname_csv_prefix = os.path.splitext(os.path.basename(path_bag))[0]

            # construct csv file name postfix
            fname_csv_postfix = '.'.join(topic_name.split('/'))
            while fname_csv_postfix[0] == '.':
                fname_csv_postfix = fname_csv_postfix[1:]

            # construct csv path
            path_csv = os.path.join(
                args.outdir,
                '%s.%s.csv' % (fname_csv_prefix, fname_csv_postfix)
            )

            # export topic to csv file
            export_topic_to_csv(path_bag, topic_name, path_csv, progress=True)


if __name__ == "__main__":
    main()
