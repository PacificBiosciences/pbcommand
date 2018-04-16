from .models import (JobExeError, JobResult, LogLevels,
                     ServiceResourceTypes, JobTypes, JobStates,
                     ServiceJob, ServiceEntryPoint)
# this module imports the models, so the model loading
# must be called first to avoid cyclic-dependencies
from ._service_access_layer import ServiceAccessLayer
# There's some crufty legacy naming. Using a cleaner model here
SmrtLinkClient = ServiceAccessLayer
