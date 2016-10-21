
SMRT Service commandline interface
==================================

A high level client to the SMRT Link Services is accessible from `ServiceAccessLayer` in `pbcommand.services`.

Client Layer
~~~~~~~~~~~~

Example:


.. code-block:: python

    In [1]: from pbcommand.services import ServiceAccessLayer

    In [2]: s = ServiceAccessLayer("smrtlink-alpha", 8081)

    In [3]: s.get_status()
    Out[3]:
    {u'id': u'smrtlink_analysis',
     u'message': u'Services have been up for 141 hours, 37 minutes and 13.138 seconds.',
     u'uptime': 509833138,
     u'user': u'secondarytest',
     u'uuid': u'12e1c62a-99a4-46c1-b616-a327dc38525f',
     u'version': u'0.1.8-3a66e4a'}

    In [4]: jobs = s.get_analysis_jobs()

    In [5]: j = s.get_analysis_job_by_id(3)

    In [6]: j.state, j.name
    Out[6]: ('SUCCESSFUL', 'sirv_isoseq')

    In [7]: import pbcommand; pbcommand.get_version()
    Out[7]: '0.4.9'


Commandline Tool Interface to Services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning:: This has been migrated to scala in smrtflow_. Support for the python Client layer API will remain, however the python commandline tool is no longer installed by default and will be removed in a future version.

.. _smrtflow: https://github.com/PacificBiosciences/smrtflow
