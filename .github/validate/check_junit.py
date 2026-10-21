#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Judge a Cassandra unit-test run from its JUnit XML (fork validation helper, not part of any patch).

The test wrappers exit 0 even when tests fail, so every job is judged from the XML files:

  check_junit.py pass <Class,...>
      every listed class must have results and nothing in it may fail or error.

  check_junit.py fail-before <Class.method,...> [<Class,...>]
      every listed method must fail with an AssertionError whose message shows a value at least
      100x too small (the unit bug under test). Anything else is NOT evidence of the bug: an <error>
      element, a compile error (no XML at all), a missing method, an initializationError, a failure
      of another type, or a value that is only slightly off. Other methods of the listed classes,
      and all methods of the optional second class list, must pass.

  check_junit.py sweep <dir-with-tests>
      every test class found under the directory must have results, none may fail or error.
"""
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

MIN_RATIO = 100.0
# assertTrue(String.format("%s.%s: expected %s ms (up to 20%% higher), got %s ms", ...))
MSG_A = re.compile(r'expected ([-+0-9.eE]+) ms.*?got ([-+0-9.eE]+) ms', re.S)
# assertEquals(String message, double expected, double actual, double delta)
MSG_B = re.compile(r'expected:<([-+0-9.eE]+)> but was:<([-+0-9.eE]+)>', re.S)


def load():
    files = sorted(glob.glob('build/**/TEST-*.xml', recursive=True))
    print('\n'.join(files) or 'no TEST-*.xml found under build/')
    results = {}
    for f in files:
        for case in ET.parse(f).getroot().iter('testcase'):
            cls = case.get('classname', '').rsplit('.', 1)[-1]
            name = cls + '.' + case.get('name', '').split('-')[0]
            failure, error = case.find('failure'), case.find('error')
            if failure is not None:
                results[name] = ('FAILURE', failure.get('type') or '', failure.get('message') or '')
            elif error is not None:
                results[name] = ('ERROR', error.get('type') or '', error.get('message') or '')
            elif case.find('skipped') is not None:
                results.setdefault(name, ('skipped', '', ''))
            else:
                results.setdefault(name, ('passed', '', ''))
    return results


def show(results):
    for name in sorted(results):
        status, typ, msg = results[name]
        line = status + ' ' + name
        if status in ('FAILURE', 'ERROR'):
            line += '  ' + typ + ': ' + ' '.join(msg.split())[:300]
        print(line)


def ratio_of(message):
    m = MSG_A.search(message) or MSG_B.search(message)
    if not m:
        return None
    expected, actual = float(m.group(1)), float(m.group(2))
    return expected, actual, (float('inf') if actual == 0 else expected / actual)


def common_problems(results, classes):
    problems = []
    for c in classes:
        if not any(n.startswith(c + '.') for n in results):
            problems.append('no results for ' + c + ' (compile error or class-level error is not a result)')
    problems += ['initializationError: ' + n for n in sorted(results) if n.endswith('.initializationError')]
    return problems


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 2
    mode = argv[1]
    results = load()
    show(results)
    if mode == 'pass':
        classes = argv[2].split(',')
        problems = common_problems(results, classes)
        problems += ['failed: ' + n for n in sorted(results)
                     if results[n][0] in ('FAILURE', 'ERROR') and n.split('.')[0] in classes]
    elif mode == 'sweep':
        classes = []
        for path in sorted(glob.glob(argv[2].rstrip('/') + '/*Test.java')):
            name = os.path.basename(path)[:-5]
            if re.search(r'\babstract\s+class\s+' + re.escape(name) + r'\b', open(path).read()):
                print('skipping abstract class ' + name)
                continue
            classes.append(name)
        print('classes under ' + argv[2] + ': ' + str(len(classes)))
        problems = common_problems(results, classes)
        problems += ['failed: ' + n for n in sorted(results) if results[n][0] in ('FAILURE', 'ERROR')]
    elif mode == 'fail-before':
        expected = argv[2].split(',')
        others = argv[3].split(',') if len(argv) > 3 and argv[3] else []
        classes = sorted(set([e.split('.')[0] for e in expected] + others))
        problems = common_problems(results, classes)
        for name in expected:
            status, typ, msg = results.get(name, ('missing', '', ''))
            if status != 'FAILURE':
                problems.append('expected an assertion failure but got ' + status + ': ' + name)
                continue
            if not typ.endswith('AssertionError') and not typ.endswith('AssertionFailedError'):
                problems.append('failure is not an assertion failure (' + typ + '): ' + name)
                continue
            r = ratio_of(msg)
            if r is None:
                problems.append('failure message does not show expected/actual values: ' + name + ': ' + ' '.join(msg.split())[:200])
            elif r[2] < MIN_RATIO:
                problems.append('value is not off by >= %.0fx (expected %s, got %s): %s' % (MIN_RATIO, r[0], r[1], name))
            else:
                print('EVIDENCE %s: expected %s, got %s, %s too small' % (
                    name, r[0], r[1], 'infinitely' if r[2] == float('inf') else '%.0fx' % r[2]))
        problems += ['unexpected failure: ' + n for n in sorted(results)
                     if results[n][0] in ('FAILURE', 'ERROR') and n not in expected and n.split('.')[0] in classes]
    else:
        print('unknown mode ' + mode)
        return 2
    print('\n'.join(problems) or 'OK')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
