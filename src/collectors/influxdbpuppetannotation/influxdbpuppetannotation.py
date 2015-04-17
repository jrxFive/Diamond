#!/usr/bin/env python

"""
Collect stats from puppet agent's last_run_summary.yaml
Send to InfluxDB for annotation usage

#### Dependencies

 * yaml

"""

try:
    import yaml
    import os
    from influxdb import client as influxdb

    yaml  # workaround for pyflakes issue #13
except ImportError:
    yaml = None

import diamond.collector


class InfluxdbPuppetAnnotation(diamond.collector.Collector):
    def get_default_config_help(self):
        config_help = super(InfluxdbPuppetAnnotation,
                            self).get_default_config_help()
        config_help.update({
            'yaml_path': "Path to last_run_summary.yaml",
            'save_file_path': "Path to store last_run timestamp",
            'influxdb_host': "InfluxDB IP",
            'influxdb_port': "InfluxDB API port",
            'influxdb_user': "InfluxDB username for specified database",
            'influxdb_pass': "InfluxDB password for specified database",
            'influxdb_database': "InfluxDB specified database\
             to send annotations to"
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(
            InfluxdbPuppetAnnotation, self).get_default_config()
        config.update({
            'yaml_path': '/var/lib/puppet/state/last_run_summary.yaml',
            'save_file_path': "/tmp/puppetagentrun",
            'influxdb_host': "localhost",
            'influxdb_port': "8086",
            'influxdb_user': "root",
            'influxdb_pass': "root",
            'influxdb_database': "diamond",
            'method': 'Threaded'
        })
        return config

    def _get_summary(self):
        try:
            summary_fp = open(self.config['yaml_path'], 'r')
            summary = yaml.load(summary_fp)
        except:
            self.log.error("Unable to open or load\
             {file}".format(file=self.config['yaml_path']))
        finally:
            summary_fp.close()

        return summary

    def get_lastrun_fromfile(self, puppet_yaml_dict):

        if os.path.exists(self.config['save_file_path']):
            try:
                fh = open(self.config['save_file_path'], 'r')
                puppet_lastrun_value = int(fh.readline())
                fh.close()
            except:
                self.log.error("Unable to open\
                 {file}".format(file=self.config['save_file_path']))
        else:
            puppet_lastrun_value = int(
                puppet_yaml_dict['time']['last_run'])

        return puppet_lastrun_value

    def update_lastrun_file(self, lastrun_value):

        with open(self.config['save_file_path'], 'w+') as fh:
            try:
                fh.write("{0}".format(lastrun_value))
            except:
                self.log.error("Unable to write to\
                 {file}".format(file=self.config['save_file_path']))

    def write_to_influxdb(self, influxdb_connection, text):

        series = "{0}".format(self.get_metric_path("puppet"))
        title = "PUPPET CONFIGURATION CHANGE"

        data = [
            {
                "points": [[title, text]],
                "name": series,
                "columns": ["title", "text"]
            }
        ]

        try:
            influxdb_connection.write_points(data)
        except:
            self.log.error("Unable to write points\
             to specified influxdb host")

    def collect(self):
        if yaml is None:
            self.log.error('Unable to import yaml')
            return

        current = self._get_summary()

        puppet_lastrun_value = self.get_lastrun_fromfile(current)
        self.update_lastrun_file(current['time']['last_run'])

        currTotChanges = current['changes']['total']
        currLastRun = current['time']['last_run']

        if currTotChanges != 0 and puppet_lastrun_value == currLastRun:
            self.log.info("No changes in puppet run")
        elif currTotChanges != 0 and puppet_lastrun_value != currLastRun:
            self.log.info("Changes in puppet run")
            db = influxdb.InfluxDBClient(self.config["influxdb_host"],
                                         self.config["influxdb_port"],
                                         self.config["influxdb_user"],
                                         self.config["influxdb_pass"],
                                         self.config["influxdb_database"])

            self.write_to_influxdb(db, "Version Configuration - \
            {0}".format(currLastRun))
