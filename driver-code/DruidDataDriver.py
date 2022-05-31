#
# DruidDataDriver - generates JSON records as a workload for Apache Druid.
#

import argparse
from dateutil import parser
from datetime import datetime
import json
from kafka import KafkaProducer
import numpy as np
import os
import random
import string
import threading
import time

############################################################################
#
# DruidDataDriver simulates Druid workloads by producing JSON records.
# Use a JSON config file to describe the characteristics of the workload
# you want to simulate.
#
# Run the program as follows:
# python DruidDataDriver.py <config file name> <options>
# Options include:
# -n <total number of records to generate>
# -t <duration for generating records>
#
# See the associated documentation for the format of the config file.
#
############################################################################


#
# Parse the command line
#

parser = argparse.ArgumentParser(description='Generates JSON records as a workload for Apache Druid.')
parser.add_argument('config_file', metavar='<config file name>', help='the workload config file name')
parser.add_argument('-t', dest='time', nargs='?', help='the script runtime (may not be used with -n)')
parser.add_argument('-n', dest='n_recs', nargs='?', help='the number of records to generate (may not be used with -t)')

args = parser.parse_args()

config_file_name = args.config_file
runtime = args.time
if args.n_recs is not None:
    total_recs = int(args.n_recs)

if (runtime is not None) and (total_recs is not None):
    print("Use either -t or -n, but not both")
    parser.print_help()
    exit()


with open(config_file_name, 'r') as f:
    config = json.load(f)

#
# Set up the target
#

class PrintStdout:
    def print(self, record):
        print(str(record))

class PrintFile:
    f = None
    def __init__(self, file_name):
        self.f = open(file_name, 'w')
    def print(self, record):
        self.f.write(str(record)+'\n')
        self.f.flush()

class PrintKafka:
    producer = None
    topic = None
    def __init(self, endpoint, topic):
        self.producer = KafkaProducer(bootstrap_servers=endpoint)
        self.topic = topic
    def print(self, record):
        self.producer.send(self.topic, str(record))

target = config['target']

if target['type'].lower() == 'stdout':
    target_printer = PrintStdout()
elif target['type'].lower() == 'file':
    path = target['path']
    if path is None:
        print('Error: File target requires a path item')
        exit()
    target_printer = PrintFile(path)
elif target['type'].lower() == 'kafka':
    endpoint = target['endpoint']
    topic = target['topic']
    if endpoint is None:
        print('Error: Kafka target requires an endpoint item')
        exit()
    if topic is None:
        print('Error: Kafka target requires a topic item')
        exit()
    target_printer = PrintKafka(endpoint, topic)
else:
    print('Error: Unknown target type "'+target['type']+'"')
    exit()


#
# Handle distributions
#

class DistConstant:
    def __init__(self, value):
        self.value = value
    def get_sample(self):
        return self.value

class DistUniform:
    def __init__(self, min_value, max_value):
        self.min_value = min_value
        self.max_value = max_value
    def get_sample(self):
        n = np.random.uniform(self.min_value, self.max_value)
        return np.random.uniform(self.min_value, self.max_value+1)

class DistExponential:
    def __init__(self, mean):
        self.mean = mean
    def get_sample(self):
        return np.random.exponential(scale = self.mean)

class DistNormal:
    def __init__(self, mean, stddev):
        self.mean = mean
        self.stddev = stddev
    def get_sample(self):
        return np.random.normal(self.mean, self.stddev)

def parse_distribution(desc):
    dist_type = desc['type'].lower()
    dist_gen = None
    if dist_type == 'constant':
        value = desc['value']
        dist_gen = DistConstant(value)
    elif dist_type == 'uniform':
        min_value = desc['min']
        max_value = desc['max']
        dist_gen = DistUniform(min_value, max_value)
    elif dist_type == 'exponential':
        mean = desc['mean']
        dist_gen = DistExponential(mean)
    elif dist_type == 'normal':
        mean = desc['mean']
        stddev = desc['stddev']
        dist_gen = DistNormal(mean, stddev)
    else:
        print('Error: Unknown distribution "'+dist_type+'"')
        exit()
    return dist_gen

def parse_timestamp_distribution(desc):
    dist_type = desc['type'].lower()
    dist_gen = None
    if dist_type == 'constant':
        value = parser.isoparse(desc['value']).timestamp()
        dist_gen = DistConstant(value)
    elif dist_type == 'uniform':
        min_value = parser.isoparse(desc['min']).timestamp()
        max_value = parser.isoparse(desc['max']).timestamp()
        dist_gen = DistUniform(min_value, max_value)
    elif dist_type == 'exponential':
        mean = parser.isoparse(desc['mean']).timestamp()
        dist_gen = DistExponential(mean)
    elif dist_type == 'normal':
        mean = desc[parser.isoparse(desc['mean']).timestamp()]
        stddev = desc['stddev']
        dist_gen = DistNormal(mean, stddev)
    else:
        print('Error: Unknown distribution "'+dist_type+'"')
        exit()
    return dist_gen

#
# Set up the rate
#

rate = config['rate']
rate_delay = parse_distribution(rate)

#
# Set up the dimensions
# There is one element class for each dimension type. This code creates a list of
# elements and then runs throught the list to create a single record.
# Notice that the get_json_field_string() method produces the JSON dimension
# field object based on the dimension configuration.
# The get_stochastic_value() method is like a private method used to get a random
# idividual value.
#

class ElementNow: # The __time dimension
    def get_json_field_string(self):
        now = datetime.now().isoformat()
        return '{"name": "__time", "value": "'+now+'"}'


class ElementBase: # Base class for the remainder of the dimensions
    def __init__(self, desc):
        self.name = desc['name']
        cardinality = desc['cardinality']
        if cardinality == 0:
            self.cardinality = None
            self.cardinality_distribution = None
        else:
            self.cardinality = []
            if 'cardinality_distribution' not in desc.keys():
                print('Element '+self.name+' specifies a cardinality without a cardinality distribution')
                exit()
            self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])
            for i in range(cardinality):
                Value = None
                while True:
                    value = self.get_stochastic_value()
                    if value not in self.cardinality:
                        break
                self.cardinality.append(value)

    def get_stochastic_value(self):
        pass

    def get_json_field_string(self):
        if self.cardinality is None:
            value = self.get_stochastic_value()
        else:
            index = int(self.cardinality_distribution.get_sample())
            if index < 0:
                index = 0
            if index >= len(self.cardinality):
                index = len(self.cardinality)-1
            value = self.cardinality[index]
        return '{"name": "'+self.name+'", "value": '+str(value)+'}'

class ElementString(ElementBase):
    def __init__(self, desc):
        self.length_distribution = parse_distribution(desc['length_distribution'])
        if 'chars' in desc:
            self.chars = desc['chars']
        else:
            self.chars = string.printable
        super().__init__(desc)

    def get_stochastic_value(self):
        length = int(self.length_distribution.get_sample())
        return ''.join(random.choices(list(self.chars), k=length))

    def get_json_field_string(self):
        if self.cardinality is None:
            value = self.get_stochastic_value()
        else:
            index = int(self.cardinality_distribution.get_sample())
            if index < 0:
                index = 0
            if index >= len(self.cardinality):
                index = len(self.cardinality)-1
            value = self.cardinality[index]
        return '{"name": "'+self.name+'", "value": "'+str(value)+'"}'

class ElementInt(ElementBase):
    def __init__(self, desc):
        self.value_distribution = parse_distribution(desc['distribution'])
        super().__init__(desc)

    def get_stochastic_value(self):
        return int(self.value_distribution.get_sample())

class ElementFloat(ElementBase):
    def __init__(self, desc):
        self.value_distribution = parse_distribution(desc['distribution'])
        super().__init__(desc)

    def get_stochastic_value(self):
        return float(self.value_distribution.get_sample())

class ElementTimestamp(ElementBase):
    def __init__(self, desc):
        self.name = desc['name']
        self.value_distribution = parse_timestamp_distribution(desc['distribution'])
        cardinality = desc['cardinality']
        if cardinality == 0:
            self.cardinality = None
            self.cardinality_distribution = None
        else:
            if 'cardinality_distribution' not in desc.keys():
                print('Element '+self.name+' specifies a cardinality without a cardinality distribution')
                exit()
            self.cardinality = []
            self.cardinality_distribution = parse_distribution(desc['cardinality_distribution'])
            for i in range(cardinality):
                Value = None
                while True:
                    value = self.get_stochastic_value()
                    if value not in self.cardinality:
                        break
                self.cardinality.append(value)

    def get_stochastic_value(self):
        return datetime.fromtimestamp(self.value_distribution.get_sample())

dimensions = config['dimensions']

elements = [ElementNow()]
for element in dimensions:
    if element['type'].lower() == 'string':
        el = ElementString(element)
    elif element['type'].lower() == 'int':
        el = ElementLong(element)
    elif element['type'].lower() == 'float':
        el = ElementFloat(element)
    elif element['type'].lower() == 'timestamp':
        el = ElementTimestamp(element)
    else:
        print('Error: Unknown dimension type "'+element['type']+'"')
        exit()
    elements.append(el)

#
# Run the driver
#

record_count = 0

def create_record(dimensions):
    json_string = '['
    for element in dimensions:
        json_string += element.get_json_field_string() + ','
    json_string = json_string[:-1] + ']'
    return json_string

thread_end_event = threading.Event()

def worker_thread(target_printer, rate_delay, dimensions):
    # Create records using worker threads (one per core) in case we need to run faster than what a single core can handle
    global record_count
    global total_recs
    global thread_end_event
    # Scale the rate for the number of cores (assuming one thread per core)
    n_cpus = os.cpu_count()
    while True:
        record = create_record(dimensions)
        target_printer.print(record)
        record_count += 1
        if total_recs is not None and record_count >= total_recs:
            thread_end_event.set()
            break
        time.sleep(float(rate_delay.get_sample()) * n_cpus)

def spawning_thread(target_printer, rate_delay, dimensions):
    # Spawn the workers in a separate thread so we can stop the whole thing in the middle of spawning if necessary
    n_cpus = os.cpu_count()
    for cpu in range(n_cpus):
        t = threading.Thread(target=worker_thread, args=(target_printer, rate_delay, elements, ), daemon=True)
        t.start()
        time.sleep(float(rate_delay.get_sample()))

t = threading.Thread(target=spawning_thread, args=(target_printer, rate_delay, elements, ), daemon=True)
t.start()

if runtime is not None:
    if runtime[-1].lower() == 's':
        t = int(runtime[:-1])
    elif runtime[-1].lower() == 'm':
        t = int(runtime[:-1]) * 60
    elif runtime[-1].lower() == 'h':
        t = int(runtime[:-1]) * 60 * 60
    else:
        print('Error: Unknown runtime value"'+runtime+'"')
        exit()
    time.sleep(t)
elif total_recs is not None:
    thread_end_event.wait()
else:
    while True:
        time.sleep(60)
