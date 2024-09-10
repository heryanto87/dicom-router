FROM --platform=linux/amd64 continuumio/miniconda3:23.5.2-0-alpine

# Update and upgrade the package manager
RUN apk update && apk upgrade --no-cache

WORKDIR /app

# Make RUN commands use `bash --login`:
SHELL ["/bin/bash", "--login", "-c"]

# Create the environment:
COPY ./conda_env/conda-unix.yml .
COPY . .
ADD router.conf router.conf

# Install the environment
RUN conda env create -n dicom-router -f conda-unix.yml

# Initialize conda in bash config files:
# RUN conda init bash

# Create a non-root user and switch to it
RUN addgroup -S dicom && adduser -S router -G dicom

# Change ownership of the /app directory to the non-root user
RUN chown -R router:dicom /app

# Switch to non-root user
USER router

# Activate the environment, and make sure it's activated:
# It's important to note that Docker containers don't persist state between RUN commands,
# so you can't activate the conda environment in one RUN command and expect it to be active in the next.
# Instead, activation should be done in the entrypoint script or as part of the CMD command.
RUN echo "conda activate dicom-router" > ~/.bashrc

EXPOSE 11112
EXPOSE 8081
EXPOSE 8082

# The code to run when container is started:
# Ensure that the copied files are owned by the non-root user
COPY --chown=router:dicom main.py entrypoint.sh ./

# Define the shared folder path
VOLUME ["/shared"]

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
