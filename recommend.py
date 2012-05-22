#! /usr/bin/python
"""
Selects resources from a Kasabi dataset and analyses them to determine
a list of similar resources for each selected resource.

Usage:

This script takes the following parameters:

  -d/--dataset    the dataset containing the geopoints
  -f/--file       the name of the output file
  -p/--property   a property to use for comparison (may be repeated)
  -t/--type       a type used for restricting the resource selection
  -q/--query      a query to select resources (may be repeated)
  -u/--uripattern a URI pattern for similarity groups
  -b/--batchsize  retrieve query results in batches of this size
  -n/--nummatches maximum number of matches per resource (default 10)
  -w/--weight     weight a query variable by the given amount e.g. p1=5 (may be repeated)

The sparql query must be a select and must select the subject of the resource in 
a variable called ?s. All other variables selected are used to compute similarity
features so only select the variables you actually want to use in this way.

Use weights to boost the importance of one or more variables

The URI pattern must have an embedded %s that denotes where the unique id for the group
should be inserted, e.g. http://example.com/groups/%s (default is _:%s)
 
Prerequisites:

Set an environment variable KASABI_API_KEY equal to your Kasabi api key. On
Linux you can do this by adding this line to your ~/.profile file:

export KASABI_API_KEY=blah

You also need to install ijson which is a python wrapper for the yajl streaming json parser.

ijson: http://pypi.python.org/pypi/ijson/
yajl: http://lloyd.github.com/yajl/


Install ruby and cmake
  
    sudo apt-get install ruby
    sudo apt-get install cmake


Download the version 1 source of yajl (not version 2), unpack and in
the source directory type:

    sudo ./configure
    sudo make install
    sudo ldconfig 
    
Install ijson with:

    sudo easy_install ijson


"""
import sys
import os
import re
import optparse
import datetime
import urllib2
import urllib
import hashlib
import pickle
import os.path
import pytassium
import numpy
import math
import time
from hcluster import pdist, squareform
from rdfgenutils import triple

import StringIO
from ijson import items

def is_number(s):
  try:
      float(s)
      return True
  except ValueError:
      return False

def unique_values(array):
  set = {}
  map(set.__setitem__, array, [])
  return set.keys()

def manhattan(vector1, vector2):
  """Computes the Manhattan distance."""
  distance = 0
  total = 0
  n = len(vector1)
  for i in range(n):
    distance += abs(vector1[i] - vector2[i])
  return distance

def computeNearestNeighbor(itemName, itemVector, items, num):
  """creates a sorted list of items based on their distance to item"""
  distances = []
  for otherItem in items:
    if otherItem != itemName:
      distance = manhattan(itemVector, items[otherItem])
      distances.append((distance, otherItem))
  
  # sort based on distance -- closest first
  distances.sort()
  return distances[:num]


p = optparse.OptionParser()
p.add_option("-d", "--dataset", action="store", dest="dataset", metavar="DATASET", help="use dataset DATASET")
p.add_option("-p", "--property", action="append", dest="property", metavar="PROPERTY", help="use sparql PROPERTY")
p.add_option("-t", "--type", action="store", dest="type", metavar="TYPE", help="use sparql TYPE")
p.add_option("-f", "--file", action="store", dest="filename", metavar="FILENAME", help="output results to FILENAME")
p.add_option("-q", "--query", action="append", dest="queries", metavar="QUERY", help="output results to QUERY")
p.add_option("-b", "--batchsize", action="store", dest="batchsize", metavar="BATCHSIZE", help="retrieve results in batches of BATCHSIZE", default=5000)
p.add_option("-u", "--uripattern", action="store", dest="uripattern", metavar="URIPATTERN", help="use a URI pattern of URIPATTERN", default="_:%s")
p.add_option("-n", "--nummatches", action="store", dest="nummatches", metavar="NUMMATCHES", help="maximum of NUMMATCHES matches", default=10)
p.add_option("-w", "--weight", action="append", dest="weights")
opts, args = p.parse_args()

pnames = {}
rows = []

if not opts.dataset:
  print "Please supply the name of a kasabi dataset and make sure you're subscribed to its sparql endpoint"
  sys.exit(1)

if not opts.filename:
  print "Please supply the name of the output file"
  sys.exit(1)

if opts.queries:
  queries = opts.queries
else:
  if not opts.property:
    print "Please supply at least one property URI"
    sys.exit(1)
  else:

    vars = "?s"
    patts = "?s"

    if opts.type:
      patts += " a <%s> ." % opts.type
    else:
      patts += " ?dummy [] ." % opts.type

    for p in range(0, len(opts.property)):
      vars += " ?p%d" % p
      patts += " optional {?s <%s> ?p%d .}" % (opts.property[p], p)
    queries = "select %s {%s }" % (vars, patts)

if not isinstance(queries, list):
  queries = [queries]


weights = {}
if opts.weights:
  for w in opts.weights:
    parts = w.split("=")
    if len(parts) != 2:
      print "Weights must be in the format var=number"
      sys.exit(1)
    if not is_number(parts[1]):
      print "Weight must be a number"
      sys.exit(1)
     
    weights[parts[0]] = float(parts[1])

source_id = hashlib.md5(opts.dataset + "\t".join(queries)).hexdigest()
if os.path.isfile("%s_rows.cache" % source_id) and os.path.isfile("%s_pnames.cache" % source_id):
  print "Reading data from cache files with id %s" % source_id
  
  fin = open("%s_rows.cache" % source_id, "r")
  rows = pickle.load(fin)
  fin.close()

  fin = open("%s_pnames.cache" % source_id, "r")
  pnames = pickle.load(fin)
  fin.close()

else:

  ds = pytassium.Dataset(opts.dataset, os.environ['KASABI_API_KEY'])
  sparql_uri = ds.get_api('sparql').uri

  for query_base in queries:

    for i in range(0, 2000000, opts.batchsize):
      batchrows = 0
      query = query_base + " limit %d offset %d" % (opts.batchsize, i)


      print "Querying for resources in %s" % opts.dataset
      print "Query: %s" % query

      url = "%s?query=%s&apikey=%s" % (sparql_uri, urllib.quote_plus(query), os.environ['KASABI_API_KEY'])

      req = urllib2.Request(url, None, {'Accept':'application/json'})
      f = urllib2.urlopen(req)
      objects = items(f, 'results.bindings.item')


      # Read all the results
      for t in objects:
        batchrows += 1
        rows.append(t)
        for p in t:
          if p != 's' and p not in pnames:
            pnames[p] = { 'numeric': True, 'values':[]}

      print "Read %d rows of data so far" % len(rows)
      if batchrows < opts.batchsize:
        break

  print "Writing data to cache files with id %s" % source_id
  fout = open("%s_rows.cache" % source_id, "w")
  pickle.dump(rows,fout)
  fout.close()

  fout = open("%s_pnames.cache" % source_id, "w")
  pickle.dump(pnames,fout)
  fout.close()


print "Read %d rows of data in total" % len(rows)

for p in pnames:
  if p in weights:
    pnames[p]['weight'] = weights[p]
  else:
    pnames[p]['weight'] = 1.0

# Determine the features
# Numeric properties are normalised on a range of 0 to 1 using Modified Standard Score
# Other properties are transformed into one feature per property value, 0 = absence of
#   that value, 1 = presence of that value

for t in rows:
  for p in pnames:
    if p in t:
      pnames[p]['values'].append(t[p]['value'])
      if not is_number(t[p]['value']):
        pnames[p]['numeric'] = False

#features = []
property_features = {}
feature_count = 0
print "Analyzing property types"
for p in pnames:

  if pnames[p]['numeric'] == True:
    #feature = {'property': p, 'type': 'numeric'}
    floatlist = sorted([float(x) for x in pnames[p]['values']])
    median = floatlist[len(floatlist)/2]
    asd = 0
    for f in floatlist:
      asd += math.fabs(f - median)
    asd = asd / len(floatlist)

    #feature['median'] = floatlist[len(floatlist)/2]
    #features.append(feature)
    feature_count += 1
    property_features[p] = [feature_count, floatlist[len(floatlist)/2], asd]
  else:

    property_features[p] = {}
    for v in unique_values(pnames[p]['values']):
      feature_count += 1
      property_features[p][v] = feature_count - 1
#      features.append( {'property': p, 'type': 'discrete', 'value':v} )

print "Found %d features" % feature_count

resources = {}
resource_index = []
resource_features = []
for t in rows:
  s = t['s']['value']
  if s not in resources:
    snum = len(resource_features)
    resources[s] = snum
    resource_index.append(s)
    resource_features.append( [float(0.0)] * feature_count ) # initialise all to zero
  else:
    snum = resources[s]

  for p in t:
    if p != 's':
      if isinstance(property_features[p], dict):
        feature_index = property_features[p][t[p]['value']]
        resource_features[snum][feature_index] = pnames[p]['weight']
      else:
        feature_index = property_features[p][0]
        median = property_features[p][1]
        asd = property_features[p][2]
        resource_features[snum][feature_index] = (float(t[p]['value']) - median)/asd

  # for f in range(0, len(features)):
  #   if features[f]['property'] in t:
  #     if features[f]['type'] == 'numeric':
  #       pass
  #     elif features[f]['type'] == 'discrete':
  #       if t[features[f]['property']]['value'] == features[f]['value']:
  #         resource_features[snum][f] = float(1.0)
      

print "Found %d distinct resources" % len(resources)

rows = None
time.sleep(10)

print "Computing distances"
distances = squareform(pdist(resource_features))
sorted_distance_args = numpy.argsort(distances)

print "Writing arff with id %s" % source_id
fout = open("%s.arff" % source_id, "w")
fout.write( "%% Similar resources generated by %s\n" % __file__ )
fout.write( "%% Date: %s\n" % datetime.datetime.today().isoformat() )
fout.write( "%% Source dataset: %s\n" % opts.dataset)
for query_orig in queries:
  fout.write( "%% Query: %s\n" % query_orig)
fout.write( "%% Found %d distinct resources\n" % len(resources))
if opts.weights:
  fout.write( "%% Weights: %s\n" % ", ".join(opts.weights))

fout.write( "@DATA")
for r in resources:
  index = resources[r]
  fout.write(r)
  for i in range(0, len(resource_features[index])):
    fout.write(",%s" % resource_features[index][i])
  fout.write("\n")
fout.close()


print "Writing results to %s" % opts.filename
fout = open(opts.filename, "w")
fout.write( "# Similar resources generated by %s\n" % __file__ )
fout.write( "# Date: %s\n" % datetime.datetime.today().isoformat() )
fout.write( "# Source dataset: %s\n" % opts.dataset)
for query_orig in queries:
  fout.write( "# Query: %s\n" % query_orig)
fout.write( "# Found %d distinct resources\n" % len(resources))
if opts.weights:
  fout.write( "# Weights: %s\n" % ", ".join(opts.weights))


nummatches = opts.nummatches
if nummatches > len(resources) - 1:
  nummatches = len(resources) - 1

std = numpy.std(distances)

for r in resources:
  index = resources[r]
  try:
    node = opts.uripattern % hashlib.md5(r).hexdigest()
    fout.write( triple(r, 'http://vocab.org/terms/similarThings', node) )
    fout.write( triple(node, 'a', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq') )
    score_base = distances[index][sorted_distance_args[index][1]]
    for i in range(1,nummatches + 1):
      score = distances[index][sorted_distance_args[index][i]]
      if score > score_base + std:
        break
  #    print "match %s has score %s (%s)" % (i, distances[index][sorted_distance_args[index][i]] , resource_index[sorted_distance_args[index][i]])
      try:
        fout.write( triple(node, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_%d' % (i), resource_index[sorted_distance_args[index][i]]) )
      except UnicodeEncodeError:  
        print "Got unicode error writing %s" % resource_index[sorted_distance_args[index][i]]
  except UnicodeEncodeError:  
    print "Got unicode error writing %s" % r

  fout.flush()
fout.close()
