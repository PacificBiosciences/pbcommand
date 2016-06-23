Report Models
-------------

A report is composed of model objects whose classes are defined in pbreports.model. Typically, a report object is created and then attributes, tables, or plotGroups are
added to the report. Lastly, the report is serialized as json to a file.

The objects that comprise a report extend BaseReportElement. `All report elements have an id`.

When the report is converted to a dictionary before serialization, each report element's id is prepended with its parent id,
which has also been prepended. For example, given the nested elements of report --> plotGroup --> plot, with respective ids "r", "pg", and "p",
the plot id would be "r.pg.p" in the dictionary.

This allows elements of a report to be looked up id, such as "mapping_stats.n_mapped_bases" for a report Attribute (i.e., metric), or a specific plot group, such as "filter_report.filtered_subreads".

.. note:: Once a report element id has been assigned, it should not change.

Report
------

Report is the root class of the model hierarchy. It's instantiated with an id (should be a short string), which defines its namespace. 
This example shows how a report is with one attribute, plotGroup, and table is created and written.

.. code-block:: python

    import os
    import logging

    from pbcommand.models.report import Report, Attribute, PlotGroup, Table

    log = logging.getLogger(__name__)
   
    def make_report():
        """Write a simple report"""
        table = create_table() # See example below
        attribute = create_attribute() # See example below
        plotGroup = create_plotGroup() # See example below

        # Id must match ^[a-z0-9_]+$
        r = Report('loading', title="Loading Report",
                attributes=[attribute],
                plotgroups=[plotGroup],
                tables=[table])

        # Alternatively
        r.add_table(table)
        r.add_attribute(attribute)
        r.add_plotGroup(plotGroup)

        r.write_json('/my/file.json')
            

Attribute
---------

An attribute represents a key-value pair with an optional name. The id is the key. A report contains
a list of attributes.

.. code-block:: python

    import os
    import logging

    from pbcommand.models.report import Attribute

    log = logging.getLogger(__name__)
   
    def create_attribute():
        """Return an attribute"""
        a = Attribute('alpha', 1234, name='Alpha')
        b = Attribute('beta', "value", name="Beta Display Name")
        return a
            

Table
-----

A table contains a list of column objects and has an optional title and id. A report contains a list of tables.
In general, the paradigm for creating a table is to instantiate a table and a series of columns. Add the 
columns to the table in the desired order. Finally, iterate over your data set and append data to the
columns by index.
 
.. code-block:: python

    import os
    import logging
    import random

    from pbcommand.models.report import Attribute, Table, Column

    log = logging.getLogger(__name__)
   
    def create_table():
        """Return a table with 2 columns"""
        columns = [Column( 'c1id', header='C1 header'),
                Column('c2id', header='C2 header')]

        t = Table('myid', title='My Table', columns=columns)

        #Now append data to the columns
        #Assume data is a list of tuples of len == 2
        datum = [(c.id, random.random()) for c in columns]
        for column_id, value in datum:
            t.add_data_by_column_id(column_id, value)

        # Alternatively
        cx = Column("cx", header="X", values=[1,2,3,4])
        cy = Column("cy", header="Y", values=[1,4,9,16])
        t = Table("xy", title="X vs Y", columns=[cx, cy])
        return t
            
        
PlotGroup
---------

A `Plot Group` represents a logical grouping or collection of plots that convey related information, such coverage across
5 contigs. A plotGroup has an id, an optional thumbnail (to represent the group in SMRT Link in a
preview), an optional legend and a list of plots.

.. code-block:: python

    import os
    import logging

    from pbcommand.model.report import PlotGroup, Plot

    log = logging.getLogger(__name__)
   
    def create_plotGroup():
        """Return a PlotGroup with 1 plot"""
        # Image paths must be relative to the dir where the final Report is written

        plot = Plot('plot_id', image='image.png', caption='this is a plot')
        p = PlotGroup('myid', title='some title', thumbnail='image_thumb.png', plots=[plot])

        return p
            

.. note:: The image paths must be written relative to where the report JSON file will be written.

.. note:: Currently, only PNG is supported