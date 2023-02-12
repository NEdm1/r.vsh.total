#!/usr/bin/env python

############################################################################
#
# MODULE:    r.viewshed.total
# AUTHOR(S): Nagy Edmond
# PURPOSE:	 Script for generating raster maps that record the total
#                number of visible cells from each input observer point.
# COPYRIGHT: (C) 2018 by Nagy Edmond, and the GRASS Development Team
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
############################################################################

#%module
#% description: Creates a total viewshed raster map from a DEM and input points using r.viewshed.
#% keyword: raster
#% keyword: r.viewshed
#% keyword: r.viewshed.cva
#%end

#%option G_OPT_V_INPUT
#% key: vect
#% description: Input observer vector points
#% required: yes
#%end

#%option G_OPT_R_INPUT
#% key: rast
#% description: Input DEM raster
#% required: yes
#%end

#%option G_OPT_R_OUTPUT
#% key: output
#% description: Output raster map
#% required: yes
#%end

#%flag
#% key: c
#% description: Consider the curvature of the earth (current ellipsoid)
#%end

#%flag
#% key: r
#% description: Consider the effect of atmospheric refraction
#%end

#%option
#% key: observer_elevation
#% type: double
#% description: Height of observer
#%answer: 1.75
#% required : no
#%end

#%option
#% key: target_elevation
#% type: double
#% description: Height of targets
#%answer: 1.75
#% required : no
#%end

#%option
#% key: max_distance
#% type: double
#% description: Maximum visibility radius. The higher the slower
#%answer: 1000
#% required : no
#%end

#%option
#% key: memory
#% type: integer
#% description: Amount of memory to use (in MB)
#%answer: 500
#% required : no
#%end

#%option
#% key: refraction_coeff
#% type: double
#% description: Refraction coefficient (with flag -r)
#%answer: 0.14286
#% options: 0.0-1.0
#% required : no
#%end

import os
import sys
import multiprocessing as multi
import grass.script as grass
from grass.pygrass.vector import VectorTopo
from grass.pygrass.vector.geometry import Point


def main():
    options, flags = grass.parser()
    
    # setup input variables
    rast = options["rast"]
    vect = options["vect"]

    viewshed_options = {}
    for option in ('observer_elevation', 'target_elevation', 'max_distance', 'memory',
                   'refraction_coeff'):
        viewshed_options[option] = options[option]

    out = options["output"]

    # setup flagstring
    flagstring = ''
    if flags['r']:
        flagstring += 'r'
    if flags['c']:
        flagstring += 'c'

    # get the input vector points
    points = grass.read_command("v.out.ascii", flags='r', input=vect, type="point",
                                format="point", separator=",").strip()

    # read the input points and parse them
    pointList = []
    for line in points.splitlines():
        if line:
            pointList.append(line.strip().split(','))

    # initialize a list that will contain tuples made up of 3: first two floats, last one int
    totViewshData = []

    # initialize process dictionary
    process = {}

    # initialize number of threads available
    workers = multi.cpu_count()

    # check if threads are already being used
    if workers is 1 and "WORKERS" in os.environ:
        workers = int(os.environ["WORKERS"])

    if workers < 1:
        workers = 1

    # run r.viewshed for each point and append x,y float coordinates and total visible cells int
    for point in pointList:
        count = int(point[2])

        process[count] = grass.start_command("r.viewshed", quiet=True, overwrite=True, input=rast,
                                             flags=flagstring+'b', output="tempViewsh"+str(count),
                                             coordinates=point[0]+","+point[1], **viewshed_options)

        if count % workers is 0:
            for job in range(workers):
                process[count-job].wait()

    for viewsh in range(len(pointList)):

        while True:
            if not grass.find_file('tempViewsh'+str(viewsh+1), element='cell').values()[0] == '':
                break

        viewshStats = grass.read_command("r.stats", quiet=True, flags='c', input='tempViewsh'+str(viewsh+1))
        totViewshData.append((float(pointList[viewsh][0]), float(pointList[viewsh][1]),
                              int(viewshStats.splitlines()[1].split(' ')[1])))

    # create a new temporary vector and add table with relevant columns
    newVect = VectorTopo('tempVect')
    newVect.open('w', tab_name='tempVect', tab_cols=[(u'cat','INTEGER PRIMARY KEY'), (u'num','INTEGER')])

    # write points where viewshed was performed and record the number of visible cells for each
    for data in totViewshData:
        newVect.write(Point(data[0], data[1]), (data[2], ))

    newVect.table.conn.commit()
    newVect.close()

    # convert the temporary vector to raster
    grass.run_command("v.to.rast", quiet=True, overwrite=grass.overwrite(), input="tempVect",
                      output=out, use="attr", attribute_column="num")

    # remove temporary files
    grass.run_command("g.remove",  quiet=True, flags='f', type='raster', pattern="tempViewsh*")
    grass.run_command("g.remove",  quiet=True, flags='f', type='vector', name="tempVect")

    return

if __name__ == "__main__":
    sys.exit(main())
