# Install CramUnit and run a simple dev example
FROM mpkocher/docker-pacbiobase
MAINTAINER Michael Kocher

# Copy the code to container 
COPY ./ /tmp/pbcommand

# Install
RUN pip install -r /tmp/C/REQUIREMENTS.txt && pip install /tmp/pbcommand
