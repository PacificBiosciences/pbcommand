Report Models
=============

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


Report Specs
============

A parallel family of models in the same module handles specifications for
individual reports, i.e. enumerating the data items expected for each model
type, along with view metadata.  The overall structure and names of objects in
the hierarchy is identical to the Report model.  For any of the nodes in the
hierarchy, the following view metadata may be specified:

  - a UI label, usually `title` (or `name` for Attributes, `header` for table
    columns)
  - a description suitable for formal documentation or mouseover text
  - a boolean `isHidden` attribute that controls visibility

There is some redundancy between the report specifications and the actual
reports - for example the Report `title` and Attribute `name` occur in both
models.  This was due to the lack of a clear model for view metadata in previous
versions of SMRTAnalysis; the Report model may be slimmed down in the future as
the view rules are deployed and utilized.

The `pbcommand` module itself does not actually define any reports; currently
most of these are part of the `pbreports` module.

Format strings
--------------

For formatting numerical attribute and column values, we are using a
lightweight syntax based on Python's `str.format(...)` method.  If the
`format` attribute is set to `None` (`null` in JSON), the value should
simply be directly converted to string without any formatting.  (In the case
of string and boolean values, the format should always be left unset.)  More
complex operations values must match this regular expression::

  {([GMkp]{0,1})(:)([\.,]{0,1})([0-9]*)([dfg]{1})}(.*)$

The `[GMkp]` group specifies scaling - if one of these characters is present,
the value should be divided by one billion (`G`), one million (`M`), or one
thousand (`k`) before formatting, or multiplied by 100 (`p`).  The period or
comma after the colon modifies the display of floating-point and integer
values respectively.  The following characters before the closing brace
correspond to conventional format string syntax.  The format can optionally
include a suffix to be appended to the formatted value.

Examples of use::

  format_value("{:,d}", 123456)           # 123,456
  format_value("{:.2f)", 1.23456)         # 1.23
  format_value("{G:.2f} Gb", 1234567890)  # 1.23 Gb
  format_value("{p:5g}%", 0.987654321)    # 98.765%
  format_value(None, 0.987654321)         # 0.987654321
