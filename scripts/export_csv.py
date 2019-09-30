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


import os
import rosbag
import csv
import tqdm


def flatten_topic_fields(msg_type, field_prefix=''):
    """
    Flatten the fields of a topic.

    @param msg_type The message type for topic.
    @param field_prefix The prefix of the current layer of topic fields.
    @return fields The flattened topic fields.
    """

    assert(hasattr(msg_type, '__slots__'))

    fields = []

    for s in msg_type.__slots__:
        field = '.'.join(field_prefix, s)
        if not hasattr(msg_type[s], '__slots__'):
            fields.append(field)
        else:
            fields += flatten_topic_fields(msg_type[s], field_prefix=field)

    return fields


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

    # read messages from the bag file
    for (topic, msg, t) in tqdm(bag.read_messages(topics=list([topic_name]))):
        # flatten topic fields and open output csv file if necessary
        if fieldnames is None:
            # flatten the topic fields
            fieldnames = flatten_topic_fields(type(msg))

            # open csv file
            csv_file = open(path_csv, 'w')
            csv_writer = csv.DictWriter(
                csv_file, fieldnames=(['_ros_time_sec'] + fieldnames)
            )

        # extract values from the message and write to the csv file
        row = {}
        row['_ros_time_sec'] = t.to_sec()
        for fieldname in fieldnames:
            row[fieldname] = get_field_value(msg, fieldname)
        csv_writer.writerow(row)

    # close the csv file
    csv_file.close()


def parse_arguments():
    """
    Parse arguments.

    @return The object of parsed arguments.
    """

    parser = argparse.ArgumentParser(
        description='Train model.'
    )

    parser.add_argument(
        '-b', '--bag', required=True, type=str,
        help='path to the bag file'
    )

    parser.add_argument(
        '-t', '--topic', required=True, type=str,
        help='name of the topic to export'
    )

    parser.add_argument(
        '-c', '--csv', required=True, type=str,
        help='path to the csv file to export to'
    )

    args = parser.parse_args()

    return args


def main():
    """
    Main entry.
    """

    args = parse_arguments()

    export_topic_to_csv(args.bag, args.topic, args.csv)


if __name__ == "__main__":
    main()
