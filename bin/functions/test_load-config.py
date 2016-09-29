import unittest
from subprocess import call
import re
import subprocess
import time
import fcntl
from load-config import probe_hadoop_examples_jars

def shell(cmd, timeout=5):
    assert not "${" in cmd, "Error, missing configurations: %s" % ", ".join(
        re.findall("\$\{(.*)\}", cmd))
    retcode, stdout, stderr = execute_cmd(cmd, timeout)
    if retcode == 'Timeout':
        assert 0, cmd + " executed timedout for %d seconds" % timeout

    return stdout

def nonBlockRead(output):
    fd = output.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    try:
        return output.read()
    except:
        return ''


def execute_cmd(cmdline, timeout):
    """
    Execute cmdline, limit execution time to 'timeout' seconds.
    Uses the subprocess module and subprocess.PIPE.

    Raises TimeoutInterrupt
    """

    p = subprocess.Popen(
        cmdline,
        bufsize=0,  # default value of 0 (unbuffered) is best
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    t_begin = time.time()  # Monitor execution time
    seconds_passed = 0

    stdout = ''
    stderr = ''

    while p.poll() is None and (
            seconds_passed < timeout or timeout == 0):  # Monitor process
        time.sleep(0.1)  # Wait a little
        seconds_passed = time.time() - t_begin

        stdout += nonBlockRead(p.stdout)
        stderr += nonBlockRead(p.stderr)

    if seconds_passed >= timeout and timeout > 0:
        try:
            p.stdout.close()  # If they are not closed the fds will hang around until
            p.stderr.close()  # os.fdlimit is exceeded and cause a nasty exception
            p.terminate()     # Important to close the fds prior to terminating the process!
            # NOTE: Are there any other "non-freed" resources?
        except:
            pass

        return ('Timeout', stdout, stderr)

    return (p.returncode, stdout, stderr)



class ProbeHadoopExamplesTestCase(unittest.TestCase):
    def setUp(self):
        self.HibenchConf = {}
        self.HibenchConf["hibench.hadoop.hoom"] = "/tmp/hadoop_home"

    def tearDown(self):
        self.HibenchConf = None

    def test_probe_hadoop_examples_jars_apache(self):
        execute_cmd("mkdir -p " + self.HibenchConf["hibench.hadoop.hoom"] + "/share/hadoop/mapreduce")
        execute_cmd(["touch " + self.HibenchConf["hibench.hadoop.hoom"] + "/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.jar"])
        probe_hadoop_examples_jars



