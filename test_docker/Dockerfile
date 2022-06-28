FROM ubuntu:20.04
LABEL maintainer="Turbot Support <help@turbot.com>"

RUN apt-get update -y

RUN apt-get install qemu binfmt-support qemu-user-static sudo -y

# Use a constant workspace directory that can be mounted to
WORKDIR /workspace

COPY test_script.sh /usr/bin

ENTRYPOINT [ "test_script.sh" ]