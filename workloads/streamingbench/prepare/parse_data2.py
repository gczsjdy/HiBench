#!/usr/bin/env python2
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
process following data:
  8	{0:-60.196392992004334,5:620.4421901009101,14:420.4220612785746,13:185.21083185702275,15:483.72692251215295,1:594.7827813502976,3:140.3239790342253,16:3.104707691856035,9:635.8535653005378,19:322.0711157700041,11:87.66295667498484,18:857.7858889856491,17:101.49594891724111,2:921.839749304954,6:697.4655671122938,7:367.3720748762538,8:855.4795500704753,10:564.4074585413068,4:913.7870598326768,12:275.71369666459043}
  9	{0:53.780307992655864,5:670.9608085434543,14:427.8278718060577,13:-42.1599560546298,15:509.38987065684455,1:575.0478527061222,3:111.01989708300927,16:48.39876690814693,9:546.0244129369196,19:344.88758399392515,11:35.63727678698427,18:826.8387868256459,17:100.39105575653751,2:972.7568962232599,6:743.3101817500838,7:367.5321255830725,8:897.5852428056947,10:705.1143980643583,4:891.1293114411877,12:364.63401807787426}

as:

  -60 594 921 140 913 620 697 367 855 635 564 87 275 185 420 483 3 101 857 322
  53 575 972 111 891 670 743 367 897 546 705 35 364 -42 427 509 48 100 826 344
"""
import sys
scale_factor = 1 if len(sys.argv)<2 else int(sys.argv[1])
for i in sys.stdin:
    d = eval(i.split()[1])
    s = " ".join([str(int(d[x])) for x in sorted(d.keys())])
    print "\n".join([s] * scale_factor)
