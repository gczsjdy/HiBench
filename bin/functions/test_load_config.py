import unittest
import os
from load_config import *


def mkdir(dir_path):
    shell("mkdir -p " + dir_path)


def touch(file_path):
    shell("touch " + file_path)

def remove(dir_path):
    shell("rm -rf " + dir_path)


def test_probe_hadoop_examples_jars():

    def test_probe_hadoop_examples_jars_generator(jar_path):
        def test(self):
            dir_path = os.path.dirname(jar_path)
            mkdir(dir_path)
            touch(jar_path)
            probe_hadoop_examples_jars()
            answer = HibenchConf["hibench.hadoop.examples.jar"]
            self.assertEqual(answer, jar_path)

        return test

    hadoop_examples_jars_list = [["apache0", "/tmp/hadoop_home/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.jar"], ["cdh0", "/tmp/hadoop_home/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.0.jar"], ["hdp0", "/tmp/hadoop_home/hadoop-mapreduce-examples.jar"]]

    for case in hadoop_examples_jars_list:
        test_name = 'test_%s' % case[0]
        test = test_probe_hadoop_examples_jars_generator(case[1])
        setattr(ProbeHadoopExamplesTestCase, test_name, test)

class ProbeHadoopExamplesTestCase(unittest.TestCase):
    def setUp(self):
        HibenchConf["hibench.hadoop.home"] = "/tmp/hadoop_home"

    def tearDown(self):
        HibenchConf["hibench.hadoop.examples.jar"] = ""
        remove(HibenchConf["hibench.hadoop.home"])

if __name__ == '__main__':
    test_probe_hadoop_examples_jars()

    suite = unittest.TestLoader().loadTestsFromTestCase(ProbeHadoopExamplesTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
