import requests
import diamond.collector


class SolrCollector(diamond.collector.Collector):
    def get_default_config_help(self):
        config_help = super(SolrCollector, self).get_default_config_help()
        config_help.update({
            'host': "",
            'port': "",
            'stats': "Available stats: \n"
                     " - jvm (JVM information) \n"
                     " - threads (JVM threads information) \n"
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(SolrCollector, self).get_default_config()
        config.update({
            'host': 'localhost',
            'port': 8983,
            'path': 'solr',
            'stats': ['jvm', 'threads'],
        })
        return config

    def get_url(self, port, path, params_dict):
        url = "http://{0}:{1}{2}".format(self.config['host'], port, path)
        try:
            response = requests.get(url, params=params_dict, timeout=5)
            return response.json()
        except:
            self.log.error("Unable to connecto to URL, or timed out")

    def collect(self):

        port_list = []

        if isinstance(self.config['port'], str) or isinstance(self.config['port'], int):
            port_list.append(int(self.config['port']))
        elif isinstance(self.config['port'], list):
            port_list = self.config['port']
        else:
            self.log.error("Port value is not str/int or list")

        for port in port_list:
            cores = []
            metrics = {}

            cores_params = {'action': 'STATUS', 'wt': 'json'}
            response = self.get_url(port, '/solr/admin/cores', cores_params)
            if response:
                cores = response['status'].keys()
            else:
                self.log.warn(
                    "No cores found to do operations on port -\
                     {0}".format(port))

            if cores:
                # only need one to get jvm and thread information
                core_name = cores[0]
                del cores

                if 'jvm' in self.config['stats']:
                    admin_system = "/solr/{0}/admin/system".format(
                        core_name)
                    admin_system_params = {'stats': 'true', 'wt': 'json'}

                    response = self.get_url(port,
                                            admin_system,
                                            admin_system_params)

                    if response:
                        raw_mem = response['jvm']['memory']['raw']

                        for key in ('free', 'total', 'max', 'used'):
                            jvm_metric = "{0}.jvm.mem.{1}".format(port, key)
                            metrics[jvm_metric] = raw_mem[key]

                if 'threads' in self.config['stats']:
                    admin_threads = "/solr/{0}/admin/threads".format(core_name)
                    admin_threads_params = {'stats': 'true', 'wt': 'json'}

                    response = self.get_url(port,
                                            admin_threads,
                                            admin_threads_params)

                    if response:
                        system = response['system']['threadCount']

                        for key in ('current', 'peak', 'daemon'):
                            t_metric = "{0}.jvm.threads.{1}".format(port, key)
                            metrics[t_metric] = system[key]

                for key, value in metrics.items():
                    self.publish(key, value)
