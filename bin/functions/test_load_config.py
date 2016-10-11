import unittest
import os
from load_config import *


def mkdir(dir_path):
    shell("mkdir -p " + dir_path)

def touch(file_path):
    shell("touch " + file_path)

def remove(dir_path):
    shell("rm -rf " + dir_path)

def print_hint_seperator(hint):
    print("\n" + hint)
    print("--------------------------------")

def runTest(test_case):
    suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
    unittest.TextTestRunner(verbosity=2).run(suite)

def parse_conf():
    conf_root = os.path.abspath(".") + "/../../conf"
    conf_files = sorted(glob.glob(conf_root + "/*.conf"))
    # load values from conf files
    for filename in conf_files:
        log("Parsing conf: %s" % filename)
        with open(filename) as f:
            for line in f.readlines():
                line = line.strip()
                if not line:
                    continue  # skip empty lines
                if line[0] == '#':
                    continue  # skip comments
                try:
                    key, value = re.split("\s", line, 1)
                except ValueError:
                    key = line.strip()
                    value = ""
                HibenchConf[key] = value.strip()
                HibenchConfRef[key] = filename

def parse_conf_before_probe():
    parse_conf()
    waterfall_config()

def test_probe_hadoop_examples_jars():

    def test_probe_hadoop_examples_jars_generator(jar_path):
        def test(self):
            dir_path = os.path.dirname(jar_path)
            mkdir(dir_path)
            touch(jar_path)
            probe_hadoop_examples_jars()
            answer = HibenchConf["hibench.hadoop.examples.jar"]
            self.assertEqual(os.path.abspath(jar_path), os.path.abspath(jar_path))

        return test

    hadoop_examples_jars_list = [["apache0", "/tmp/test/hadoop_home/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.jar"], ["cdh0", "/tmp/test/hadoop_home/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.0.jar"],
                                 ["cdh1", "/tmp/test/hadoop_home/../../jars/hadoop-mapreduce-examples-*.jar"], ["hdp0", "/tmp/test/hadoop_home/hadoop-mapreduce-examples.jar"]]

    for case in hadoop_examples_jars_list:
        test_name = 'test_%s' % case[0]
        test = test_probe_hadoop_examples_jars_generator(case[1])
        setattr(ProbeHadoopExamplesTestCase, test_name, test)

    print_hint_seperator("Test probe hadoop examples jars:")
    runTest(ProbeHadoopExamplesTestCase)

def test_probe_hadoop_test_examples_jars():
    def test_probe_hadoop_examples_jars_generator(jar_path):
        def test(self):
            dir_path = os.path.dirname(jar_path)
            mkdir(dir_path)
            touch(jar_path)
            probe_hadoop_examples_jars()
            answer = HibenchConf["hibench.hadoop.examples.test.jar"]
            self.assertEqual(os.path.abspath(jar_path), os.path.abspath(jar_path))

        return test

    hadoop_examples_jars_list = [["apache0", "/tmp/test/hadoop_home/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"], ["cdh0", "/tmp/test/hadoop_home/share/hadoop/mapreduce2/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"],
                                 ["cdh1", "/tmp/test/hadoop_home/../../jars/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"], ["hdp0", "/tmp/test/hadoop_home/hadoop-mapreduce-client-jobclient-tests.jar"]]

    for case in hadoop_examples_jars_list:
        test_name = 'test_%s' % case[0]
        test = test_probe_hadoop_examples_jars_generator(case[1])
        setattr(ProbeHadoopTestExamplesTestCase, test_name, test)

    print_hint_seperator("Test probe hadoop test examples jars:")
    runTest(ProbeHadoopExamplesTestCase)

def test_probe_java_bin():
    print_hint_seperator("Test probe java bin:")
    runTest(ProbeJavaBinTestCase)

def test_probe_hadoop_release():
    print_hint_seperator("Test probe hadoop release:")
    runTest(ProbeHadoopReleaseTestCase)


class ProbeHadoopExamplesTestCase(unittest.TestCase):
    def setUp(self):
        HibenchConf["hibench.hadoop.home"] = "/tmp/test/hadoop_home"

    def tearDown(self):
        HibenchConf["hibench.hadoop.examples.jar"] = ""
        remove(HibenchConf["hibench.hadoop.home"])

class ProbeHadoopTestExamplesTestCase(unittest.TestCase):
    def setUp(self):
        HibenchConf["hibench.hadoop.home"] = "/tmp/test/hadoop_home"

    def tearDown(self):
        HibenchConf["hibench.hadoop.examples.test.jar"] = ""
        remove(HibenchConf["hibench.hadoop.home"])

class ProbeJavaBinTestCase(unittest.TestCase):
    def setUp(self):
        self.java_home = "/tmp/test/java_home"
        self.java_bin = self.java_home + "/bin/java"
        pass
    def tearDown(self):
        remove(self.java_home)
        pass
    def test_probe_java_bin(self):
        os.environ["JAVA_HOME"] = self.java_home # visible in this process + all children
        touch(self.java_bin)
        probe_java_bin()
        answer = HibenchConf["java.bin"]
        expected = self.java_bin
        self.assertEqual(answer, expected)

class ProbeHadoopReleaseTestCase(unittest.TestCase):
    def expected_hadoop_release(self):

        cmd_release_and_version = HibenchConf[
                                      'hibench.hadoop.executable'] + ' version | head -1'
        # An example for version here: apache hadoop 2.7.3
        hadoop_release_and_version = shell(cmd_release_and_version).strip()
        expected_hadoop_release = \
            "cdh4" if "cdh4" in hadoop_release_and_version else \
                "cdh5" if "cdh5" in hadoop_release_and_version else \
                    "apache" if "Hadoop" in hadoop_release_and_version else \
                        "UNKNOWN"
        return expected_hadoop_release

    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_probe_hadoop_release(self):
        parse_conf_before_probe()
        probe_hadoop_release()
        answer = HibenchConf["hibench.hadoop.release"]
        expected = self.expected_hadoop_release()
        self.assertEqual(answer, expected)

if __name__ == '__main__':
    test_probe_hadoop_examples_jars()
    test_probe_hadoop_test_examples_jars()
    test_probe_java_bin()
    test_probe_hadoop_release()
